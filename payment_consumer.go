package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	paymentSucceededTopic = "payment.succeeded"
	paymentFailedTopic    = "payment.failed"

	orderPaymentSucceededConsumerGroup = "order-service-payment-succeeded"

	orderPaymentFailedConsumerGroup = "order-service-payment-failed"
)

// PaymentEvent описывает результат оплаты,
// публикуемый Billing Service.
type PaymentEvent struct {
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

// consumePaymentEvents обрабатывает payment-события.
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

			Topic: topic,

			GroupID: groupID,

			MinBytes: 1,
			MaxBytes: 10e6,

			StartOffset: kafka.FirstOffset,
		},
	)
	defer reader.Close()

	app.logger.Printf(
		"Kafka consumer запущен: topic=%s group=%s broker=%s",
		topic,
		groupID,
		getKafkaBroker(),
	)

	for {
		message, err := reader.ReadMessage(ctx)

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

			time.Sleep(time.Second)

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

			continue
		}

		if event.OrderID <= 0 {
			app.logger.Printf(
				"Пропущено payment-событие с некорректным order_id=%d",
				event.OrderID,
			)

			continue
		}

		if err := app.updateOrderStatus(
			ctx,
			event.OrderID,
			orderStatus,
		); err != nil {
			app.logger.Printf(
				"Ошибка изменения статуса заказа order_id=%d status=%s: %v",
				event.OrderID,
				orderStatus,
				err,
			)

			continue
		}

		app.logger.Printf(
			"Статус заказа изменён: order_id=%d status=%s topic=%s",
			event.OrderID,
			orderStatus,
			topic,
		)
	}
}

// updateOrderStatus обновляет статус заказа.
func (app *Application) updateOrderStatus(
	parentContext context.Context,
	orderID int64,
	status string,
) error {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	_, err := app.db.Exec(
		ctx,
		`
		UPDATE orders
		SET
			status = $2,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		`,
		orderID,
		status,
	)

	return err
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
