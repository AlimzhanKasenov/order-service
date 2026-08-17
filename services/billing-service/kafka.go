package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	userCreatedTopic      = "user.created"
	orderCreatedTopic     = "order.created"
	paymentSucceededTopic = "payment.succeeded"
	paymentFailedTopic    = "payment.failed"

	billingUserCreatedConsumerGroup = "billing-service-user-created"

	billingOrderCreatedConsumerGroup = "billing-service-order-created"

	defaultKafkaBroker = "localhost:9092"

	kafkaPublishTimeout = 5 * time.Second
)

// UserCreatedEvent описывает событие создания пользователя.
type UserCreatedEvent struct {
	UserID    int64     `json:"userId"`
	Username  string    `json:"username"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"createdAt"`
}

// OrderCreatedEvent описывает событие создания заказа.
type OrderCreatedEvent struct {
	OrderID   int64     `json:"orderId"`
	UserID    int64     `json:"userId"`
	Price     int64     `json:"price"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"createdAt"`
}

// PaymentEvent описывает результат оплаты заказа.
type PaymentEvent struct {
	OrderID     int64     `json:"orderId"`
	UserID      int64     `json:"userId"`
	Price       int64     `json:"price"`
	Email       string    `json:"email"`
	Balance     int64     `json:"balance"`
	Status      string    `json:"status"`
	ProcessedAt time.Time `json:"processedAt"`
}

// consumeUserCreatedEvents слушает user.created
// и автоматически создаёт счёт пользователя.
func (app *Application) consumeUserCreatedEvents(
	ctx context.Context,
) {
	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers: []string{
				getKafkaBroker(),
			},

			Topic: userCreatedTopic,

			GroupID: billingUserCreatedConsumerGroup,

			MinBytes: 1,
			MaxBytes: 10e6,

			StartOffset: kafka.FirstOffset,
		},
	)
	defer reader.Close()

	app.logger.Printf(
		"Kafka consumer запущен: topic=%s group=%s broker=%s",
		userCreatedTopic,
		billingUserCreatedConsumerGroup,
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
				"Ошибка чтения Kafka user.created: %v",
				err,
			)

			time.Sleep(time.Second)

			continue
		}

		var event UserCreatedEvent

		if err := json.Unmarshal(
			message.Value,
			&event,
		); err != nil {
			app.logger.Printf(
				"Некорректное событие user.created: %v",
				err,
			)

			continue
		}

		if event.UserID <= 0 {
			app.logger.Printf(
				"Пропущено событие user.created с некорректным userId=%d",
				event.UserID,
			)

			continue
		}

		account, err := app.createAccount(
			ctx,
			event.UserID,
		)

		if err != nil {
			if isUniqueViolation(err) {
				app.logger.Printf(
					"Счёт пользователя %d уже существует, повторное событие пропущено",
					event.UserID,
				)

				continue
			}

			app.logger.Printf(
				"Ошибка создания счёта из user.created user_id=%d: %v",
				event.UserID,
				err,
			)

			continue
		}

		app.logger.Printf(
			"Kafka user.created обработан: user_id=%d account_id=%d balance=%d",
			account.UserID,
			account.ID,
			account.Balance,
		)
	}
}

// consumeOrderCreatedEvents слушает order.created.
//
// Если денег хватает:
//   - списывает стоимость заказа;
//   - публикует payment.succeeded.
//
// Если денег недостаточно:
//   - баланс не изменяет;
//   - публикует payment.failed.
func (app *Application) consumeOrderCreatedEvents(
	ctx context.Context,
) {
	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers: []string{
				getKafkaBroker(),
			},

			Topic: orderCreatedTopic,

			GroupID: billingOrderCreatedConsumerGroup,

			MinBytes: 1,
			MaxBytes: 10e6,

			StartOffset: kafka.FirstOffset,
		},
	)
	defer reader.Close()

	app.logger.Printf(
		"Kafka consumer запущен: topic=%s group=%s broker=%s",
		orderCreatedTopic,
		billingOrderCreatedConsumerGroup,
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
				"Ошибка чтения Kafka order.created: %v",
				err,
			)

			time.Sleep(time.Second)

			continue
		}

		var event OrderCreatedEvent

		if err := json.Unmarshal(
			message.Value,
			&event,
		); err != nil {
			app.logger.Printf(
				"Некорректное событие order.created: %v",
				err,
			)

			continue
		}

		if event.OrderID <= 0 ||
			event.UserID <= 0 ||
			event.Price <= 0 {
			app.logger.Printf(
				"Пропущено некорректное order.created: order_id=%d user_id=%d price=%d",
				event.OrderID,
				event.UserID,
				event.Price,
			)

			continue
		}

		account, sufficientFunds, err := app.withdraw(
			ctx,
			event.UserID,
			event.Price,
		)

		if err != nil {
			app.logger.Printf(
				"Ошибка оплаты заказа order_id=%d user_id=%d: %v",
				event.OrderID,
				event.UserID,
				err,
			)

			continue
		}

		if sufficientFunds {
			paymentEvent := PaymentEvent{
				OrderID:     event.OrderID,
				UserID:      event.UserID,
				Price:       event.Price,
				Email:       event.Email,
				Balance:     account.Balance,
				Status:      "SUCCESS",
				ProcessedAt: time.Now().UTC(),
			}

			if err := publishPaymentEvent(
				ctx,
				paymentSucceededTopic,
				paymentEvent,
			); err != nil {
				app.logger.Printf(
					"Ошибка публикации payment.succeeded order_id=%d: %v",
					event.OrderID,
					err,
				)

				continue
			}

			app.logger.Printf(
				"Заказ успешно оплачен: order_id=%d user_id=%d price=%d balance=%d topic=%s",
				event.OrderID,
				event.UserID,
				event.Price,
				account.Balance,
				paymentSucceededTopic,
			)

			continue
		}

		paymentEvent := PaymentEvent{
			OrderID:     event.OrderID,
			UserID:      event.UserID,
			Price:       event.Price,
			Email:       event.Email,
			Balance:     account.Balance,
			Status:      "FAILED",
			ProcessedAt: time.Now().UTC(),
		}

		if err := publishPaymentEvent(
			ctx,
			paymentFailedTopic,
			paymentEvent,
		); err != nil {
			app.logger.Printf(
				"Ошибка публикации payment.failed order_id=%d: %v",
				event.OrderID,
				err,
			)

			continue
		}

		app.logger.Printf(
			"Недостаточно средств: order_id=%d user_id=%d price=%d balance=%d topic=%s",
			event.OrderID,
			event.UserID,
			event.Price,
			account.Balance,
			paymentFailedTopic,
		)
	}
}

// publishPaymentEvent публикует результат оплаты в Kafka.
func publishPaymentEvent(
	parentContext context.Context,
	topic string,
	event PaymentEvent,
) error {
	payload, err := json.Marshal(
		event,
	)

	if err != nil {
		return fmt.Errorf(
			"failed to encode payment event: %w",
			err,
		)
	}

	writer := &kafka.Writer{
		Addr: kafka.TCP(
			getKafkaBroker(),
		),

		Topic: topic,

		Balancer: &kafka.LeastBytes{},
	}

	defer writer.Close()

	ctx, cancel := context.WithTimeout(
		parentContext,
		kafkaPublishTimeout,
	)
	defer cancel()

	err = writer.WriteMessages(
		ctx,
		kafka.Message{
			Key: []byte(
				strconv.FormatInt(
					event.OrderID,
					10,
				),
			),

			Value: payload,

			Time: event.ProcessedAt,
		},
	)

	if err != nil {
		return fmt.Errorf(
			"failed to publish %s: %w",
			topic,
			err,
		)
	}

	return nil
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
