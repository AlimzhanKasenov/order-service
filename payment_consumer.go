package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"
)

const (
	paymentSucceededTopic = "payment.succeeded"
	paymentFailedTopic    = "payment.failed"

	orderPaymentSucceededConsumerGroup = "order-service-payment-succeeded"
	orderPaymentFailedConsumerGroup    = "order-service-payment-failed"

	kafkaPaymentRetryInterval = time.Second
)

// PaymentEvent описывает результат оплаты,
// публикуемый Billing Service через Transactional Outbox.
type PaymentEvent struct {
	EventID     string    `json:"eventId"`
	OrderID     int64     `json:"orderId"`
	UserID      int64     `json:"userId"`
	Price       int64     `json:"price"`
	Email       string    `json:"email"`
	Balance     int64     `json:"balance"`
	Status      string    `json:"status"`
	ProcessedAt time.Time `json:"processedAt"`
}

// consumePaymentSucceededEvents меняет заказ на PAID.
func (app *Application) consumePaymentSucceededEvents(
	ctx context.Context,
) {
	app.consumePaymentEvents(
		ctx,
		paymentSucceededTopic,
		orderPaymentSucceededConsumerGroup,
		"PAID",
	)
}

// consumePaymentFailedEvents меняет заказ
// на PAYMENT_FAILED.
func (app *Application) consumePaymentFailedEvents(
	ctx context.Context,
) {
	app.consumePaymentEvents(
		ctx,
		paymentFailedTopic,
		orderPaymentFailedConsumerGroup,
		"PAYMENT_FAILED",
	)
}

// consumePaymentEvents обрабатывает Kafka-события
// через Inbox Pattern.
//
// Offset Kafka подтверждается только после
// успешного COMMIT PostgreSQL.
func (app *Application) consumePaymentEvents(
	ctx context.Context,
	topic string,
	groupID string,
	orderStatus string,
) {
	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers: []string{
				getKafkaBroker(),
			},
			Topic:          topic,
			GroupID:        groupID,
			MinBytes:       1,
			MaxBytes:       10e6,
			StartOffset:    kafka.FirstOffset,
			CommitInterval: 0,
		},
	)

	defer reader.Close()

	app.logger.Printf(
		"Kafka consumer запущен: topic=%s group=%s broker=%s manual_commit=true",
		topic,
		groupID,
		getKafkaBroker(),
	)

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(
				err,
				context.Canceled,
			) || ctx.Err() != nil {
				return
			}

			app.logger.Printf(
				"Ошибка чтения Kafka topic=%s: %v",
				topic,
				err,
			)

			if !waitPaymentRetry(ctx) {
				return
			}

			continue
		}

		var event PaymentEvent

		if err := json.Unmarshal(
			message.Value,
			&event,
		); err != nil {
			app.logger.Printf(
				"Некорректное payment-событие topic=%s: %v",
				topic,
				err,
			)

			if err := reader.CommitMessages(
				ctx,
				message,
			); err != nil {
				app.logger.Printf(
					"Ошибка commit некорректного payment event topic=%s: %v",
					topic,
					err,
				)
			}

			continue
		}

		if event.EventID == "" ||
			event.OrderID <= 0 {
			app.logger.Printf(
				"Пропущено некорректное payment-событие topic=%s event_id=%q order_id=%d",
				topic,
				event.EventID,
				event.OrderID,
			)

			if err := reader.CommitMessages(
				ctx,
				message,
			); err != nil {
				app.logger.Printf(
					"Ошибка commit некорректного payment event: %v",
					err,
				)
			}

			continue
		}

		for {
			duplicate, err := app.processPaymentEvent(
				ctx,
				event,
				topic,
				orderStatus,
			)

			if err != nil {
				if ctx.Err() != nil {
					return
				}

				app.logger.Printf(
					"Ошибка транзакционной обработки payment event_id=%s order_id=%d topic=%s: %v",
					event.EventID,
					event.OrderID,
					topic,
					err,
				)

				if !waitPaymentRetry(ctx) {
					return
				}

				continue
			}

			if duplicate {
				app.logger.Printf(
					"Inbox: повторное payment-событие пропущено event_id=%s order_id=%d topic=%s",
					event.EventID,
					event.OrderID,
					topic,
				)
			} else {
				app.logger.Printf(
					"Статус заказа изменён транзакционно: event_id=%s order_id=%d status=%s topic=%s",
					event.EventID,
					event.OrderID,
					orderStatus,
					topic,
				)
			}

			break
		}

		if err := reader.CommitMessages(
			ctx,
			message,
		); err != nil {
			if ctx.Err() != nil {
				return
			}

			app.logger.Printf(
				"Ошибка Kafka commit payment event_id=%s topic=%s: %v",
				event.EventID,
				topic,
				err,
			)
		}
	}
}

// processPaymentEvent атомарно:
//
//  1. регистрирует eventId в Inbox;
//  2. блокирует строку заказа;
//  3. меняет статус;
//  4. выполняет COMMIT.
//
// Если eventId уже был обработан, повторное изменение
// заказа не выполняется.
func (app *Application) processPaymentEvent(
	parentContext context.Context,
	event PaymentEvent,
	topic string,
	orderStatus string,
) (
	bool,
	error,
) {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	tx, err := app.db.BeginTx(
		ctx,
		pgx.TxOptions{
			IsoLevel: pgx.ReadCommitted,
		},
	)
	if err != nil {
		return false, fmt.Errorf(
			"failed to begin payment transaction: %w",
			err,
		)
	}

	defer tx.Rollback(ctx)

	firstProcessing, err := reserveInboxEventTx(
		ctx,
		tx,
		event.EventID,
		topic,
	)
	if err != nil {
		return false, err
	}

	if !firstProcessing {
		return true, nil
	}

	var orderID int64

	err = tx.QueryRow(
		ctx,
		`
		SELECT id
		FROM orders
		WHERE id = $1
		FOR UPDATE
		`,
		event.OrderID,
	).Scan(
		&orderID,
	)

	if err != nil {
		return false, fmt.Errorf(
			"failed to lock order %d: %w",
			event.OrderID,
			err,
		)
	}

	_, err = tx.Exec(
		ctx,
		`
		UPDATE orders
		SET
			status = $2,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		`,
		event.OrderID,
		orderStatus,
	)

	if err != nil {
		return false, fmt.Errorf(
			"failed to update order status: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf(
			"failed to commit payment transaction: %w",
			err,
		)
	}

	return false, nil
}

func waitPaymentRetry(
	ctx context.Context,
) bool {
	timer := time.NewTimer(
		kafkaPaymentRetryInterval,
	)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false

	case <-timer.C:
		return true
	}
}

// getKafkaBroker возвращает адрес Kafka.
func getKafkaBroker() string {
	broker := strings.TrimSpace(
		os.Getenv("KAFKA_BROKER"),
	)

	if broker == "" {
		return defaultKafkaBroker
	}

	return broker
}
