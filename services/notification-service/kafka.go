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

	notificationSucceededConsumerGroup = "notification-service-payment-succeeded"
	notificationFailedConsumerGroup    = "notification-service-payment-failed"

	defaultKafkaBroker = "localhost:9092"

	kafkaNotificationRetryInterval = time.Second
)

// PaymentEvent описывает событие результата оплаты,
// которое публикует Billing Service через Outbox.
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

// consumePaymentSucceededEvents слушает payment.succeeded.
func (app *Application) consumePaymentSucceededEvents(
	ctx context.Context,
) {
	app.consumePaymentEvents(
		ctx,
		paymentSucceededTopic,
		notificationSucceededConsumerGroup,
	)
}

// consumePaymentFailedEvents слушает payment.failed.
func (app *Application) consumePaymentFailedEvents(
	ctx context.Context,
) {
	app.consumePaymentEvents(
		ctx,
		paymentFailedTopic,
		notificationFailedConsumerGroup,
	)
}

// consumePaymentEvents использует Inbox Pattern.
//
// Kafka offset подтверждается только после
// успешного COMMIT PostgreSQL.
func (app *Application) consumePaymentEvents(
	ctx context.Context,
	topic string,
	groupID string,
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

			if !waitNotificationRetry(ctx) {
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
				"Некорректное Kafka-событие topic=%s: %v",
				topic,
				err,
			)

			if err := reader.CommitMessages(
				ctx,
				message,
			); err != nil {
				app.logger.Printf(
					"Ошибка commit некорректного Kafka-события: %v",
					err,
				)
			}

			continue
		}

		if event.EventID == "" ||
			event.UserID <= 0 ||
			event.OrderID <= 0 ||
			event.Email == "" {
			app.logger.Printf(
				"Пропущено некорректное payment-событие topic=%s event_id=%q order_id=%d user_id=%d",
				topic,
				event.EventID,
				event.OrderID,
				event.UserID,
			)

			if err := reader.CommitMessages(
				ctx,
				message,
			); err != nil {
				app.logger.Printf(
					"Ошибка commit некорректного payment-события: %v",
					err,
				)
			}

			continue
		}

		for {
			notification,
				duplicate,
				err := app.processPaymentNotification(
				ctx,
				event,
				topic,
			)

			if err != nil {
				if ctx.Err() != nil {
					return
				}

				app.logger.Printf(
					"Ошибка транзакционной обработки Notification event_id=%s order_id=%d: %v",
					event.EventID,
					event.OrderID,
					err,
				)

				if !waitNotificationRetry(ctx) {
					return
				}

				continue
			}

			if duplicate {
				app.logger.Printf(
					"Inbox: повторное payment-событие Notification пропущено event_id=%s order_id=%d",
					event.EventID,
					event.OrderID,
				)
			} else {
				app.logger.Printf(
					"Kafka notification сохранён транзакционно: event_id=%s topic=%s notification_id=%d order_id=%d user_id=%d status=%s",
					event.EventID,
					topic,
					notification.ID,
					notification.OrderID,
					notification.UserID,
					notification.Status,
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
				"Ошибка Kafka commit Notification event_id=%s: %v",
				event.EventID,
				err,
			)
		}
	}
}

// buildNotificationFromPaymentEvent формирует уведомление.
func buildNotificationFromPaymentEvent(
	event PaymentEvent,
) CreateNotificationRequest {
	if event.Status == "SUCCESS" {
		return CreateNotificationRequest{
			UserID:  event.UserID,
			OrderID: event.OrderID,
			Email:   event.Email,
			Subject: "Заказ успешно оплачен",
			Message: "Ваш заказ успешно оплачен.",
			Status:  "SUCCESS",
		}
	}

	return CreateNotificationRequest{
		UserID:  event.UserID,
		OrderID: event.OrderID,
		Email:   event.Email,
		Subject: "Ошибка оплаты заказа",
		Message: "Недостаточно средств для оплаты заказа.",
		Status:  "FAILED",
	}
}

func waitNotificationRetry(
	ctx context.Context,
) bool {
	timer := time.NewTimer(
		kafkaNotificationRetryInterval,
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
