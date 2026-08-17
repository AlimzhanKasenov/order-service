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

	notificationFailedConsumerGroup = "notification-service-payment-failed"

	defaultKafkaBroker = "localhost:9092"
)

// PaymentEvent описывает событие результата оплаты,
// которое публикует Billing Service.
type PaymentEvent struct {
	OrderID     int64     `json:"orderId"`
	UserID      int64     `json:"userId"`
	Price       int64     `json:"price"`
	Email       string    `json:"email"`
	Balance     int64     `json:"balance"`
	Status      string    `json:"status"`
	ProcessedAt time.Time `json:"processedAt"`
}

// consumePaymentSucceededEvents слушает payment.succeeded
// и сохраняет успешное уведомление.
func (app *Application) consumePaymentSucceededEvents(
	ctx context.Context,
) {
	app.consumePaymentEvents(
		ctx,
		paymentSucceededTopic,
		notificationSucceededConsumerGroup,
	)
}

// consumePaymentFailedEvents слушает payment.failed
// и сохраняет уведомление о неуспешной оплате.
func (app *Application) consumePaymentFailedEvents(
	ctx context.Context,
) {
	app.consumePaymentEvents(
		ctx,
		paymentFailedTopic,
		notificationFailedConsumerGroup,
	)
}

// consumePaymentEvents содержит общую логику
// обработки payment-событий.
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
				"Некорректное Kafka-событие topic=%s: %v",
				topic,
				err,
			)

			continue
		}

		if event.UserID <= 0 ||
			event.OrderID <= 0 ||
			event.Email == "" {
			app.logger.Printf(
				"Пропущено некорректное payment-событие topic=%s order_id=%d user_id=%d",
				topic,
				event.OrderID,
				event.UserID,
			)

			continue
		}

		request := buildNotificationFromPaymentEvent(
			event,
		)

		notification, err := app.createNotification(
			ctx,
			request,
		)

		if err != nil {
			app.logger.Printf(
				"Ошибка сохранения Kafka-уведомления topic=%s order_id=%d: %v",
				topic,
				event.OrderID,
				err,
			)

			continue
		}

		app.logger.Printf(
			"Kafka notification сохранён: topic=%s notification_id=%d order_id=%d user_id=%d status=%s",
			topic,
			notification.ID,
			notification.OrderID,
			notification.UserID,
			notification.Status,
		)
	}
}

// buildNotificationFromPaymentEvent формирует
// "письмо счастья" или "письмо горя".
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

			Status: "SUCCESS",
		}
	}

	return CreateNotificationRequest{
		UserID:  event.UserID,
		OrderID: event.OrderID,
		Email:   event.Email,

		Subject: "Ошибка оплаты заказа",

		Message: "Недостаточно средств для оплаты заказа.",

		Status: "FAILED",
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
