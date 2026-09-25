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
	userCreatedTopic      = "user.created"
	orderCreatedTopic     = "order.created"
	paymentSucceededTopic = "payment.succeeded"
	paymentFailedTopic    = "payment.failed"

	billingUserCreatedConsumerGroup = "billing-service-user-created"

	billingOrderCreatedConsumerGroup = "billing-service-order-created"

	defaultKafkaBroker       = "localhost:9092"
	kafkaTopicCheckTimeout   = 5 * time.Second
	kafkaTopicWaitInterval   = 2 * time.Second
	kafkaProcessingRetryWait = time.Second
)

// UserCreatedEvent описывает событие создания пользователя.
type UserCreatedEvent struct {
	EventID   string    `json:"eventId"`
	UserID    int64     `json:"userId"`
	Username  string    `json:"username"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"createdAt"`
}

// OrderCreatedEvent описывает событие создания заказа.
type OrderCreatedEvent struct {
	EventID   string    `json:"eventId"`
	OrderID   int64     `json:"orderId"`
	UserID    int64     `json:"userId"`
	Price     int64     `json:"price"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"createdAt"`
}

// PaymentEvent описывает результат оплаты заказа.
//
// eventId будет автоматически добавлен
// функцией insertOutboxEventTx.
type PaymentEvent struct {
	OrderID     int64     `json:"orderId"`
	UserID      int64     `json:"userId"`
	Price       int64     `json:"price"`
	Email       string    `json:"email"`
	Balance     int64     `json:"balance"`
	Status      string    `json:"status"`
	ProcessedAt time.Time `json:"processedAt"`
}

// waitForKafkaTopic ждёт готовности Kafka topic.
func (app *Application) waitForKafkaTopic(
	ctx context.Context,
	topic string,
) error {
	broker := getKafkaBroker()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		dialContext, cancel := context.WithTimeout(
			ctx,
			kafkaTopicCheckTimeout,
		)

		conn, err := kafka.DialContext(
			dialContext,
			"tcp",
			broker,
		)

		cancel()

		if err == nil {
			_ = conn.SetDeadline(
				time.Now().Add(
					kafkaTopicCheckTimeout,
				),
			)

			partitions, readError := conn.ReadPartitions(
				topic,
			)

			closeError := conn.Close()

			if readError == nil &&
				len(partitions) > 0 {
				app.logger.Printf(
					"Kafka topic готов: topic=%s broker=%s partitions=%d",
					topic,
					broker,
					len(partitions),
				)

				return nil
			}

			if readError != nil {
				err = readError
			} else if closeError != nil {
				err = closeError
			} else {
				err = errors.New(
					"topic has no partitions",
				)
			}
		}

		app.logger.Printf(
			"Kafka topic пока недоступен: topic=%s broker=%s error=%v",
			topic,
			broker,
			err,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-time.After(
			kafkaTopicWaitInterval,
		):
		}
	}
}

// consumeUserCreatedEvents слушает user.created.
//
// Используется FetchMessage вместо ReadMessage,
// чтобы Kafka offset подтверждался вручную только
// после успешного COMMIT PostgreSQL.
func (app *Application) consumeUserCreatedEvents(
	ctx context.Context,
) {
	if err := app.waitForKafkaTopic(
		ctx,
		userCreatedTopic,
	); err != nil {
		if errors.Is(
			err,
			context.Canceled,
		) || ctx.Err() != nil {
			return
		}

		app.logger.Printf(
			"Не удалось дождаться Kafka topic %s: %v",
			userCreatedTopic,
			err,
		)

		return
	}

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

			CommitInterval: 0,
		},
	)

	defer reader.Close()

	app.logger.Printf(
		"Kafka consumer запущен: topic=%s group=%s broker=%s manual_commit=true",
		userCreatedTopic,
		billingUserCreatedConsumerGroup,
		getKafkaBroker(),
	)

	for {
		message, err := reader.FetchMessage(
			ctx,
		)
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

			if err := commitKafkaMessage(
				ctx,
				reader,
				message,
			); err != nil {
				app.logger.Printf(
					"Ошибка commit некорректного user.created: %v",
					err,
				)
			}

			continue
		}

		if event.EventID == "" ||
			event.UserID <= 0 {
			app.logger.Printf(
				"Пропущено некорректное user.created: event_id=%q user_id=%d",
				event.EventID,
				event.UserID,
			)

			if err := commitKafkaMessage(
				ctx,
				reader,
				message,
			); err != nil {
				app.logger.Printf(
					"Ошибка commit некорректного user.created: %v",
					err,
				)
			}

			continue
		}

		for {
			account, created, duplicate, err :=
				app.processUserCreatedEvent(
					ctx,
					event,
				)

			if err != nil {
				if ctx.Err() != nil {
					return
				}

				app.logger.Printf(
					"Ошибка транзакционной обработки user.created event_id=%s user_id=%d: %v",
					event.EventID,
					event.UserID,
					err,
				)

				if !waitKafkaProcessingRetry(ctx) {
					return
				}

				continue
			}

			if duplicate {
				app.logger.Printf(
					"Inbox: повторное user.created пропущено event_id=%s user_id=%d",
					event.EventID,
					event.UserID,
				)
			} else if created {
				app.logger.Printf(
					"Kafka user.created обработан: event_id=%s user_id=%d account_id=%d balance=%d",
					event.EventID,
					account.UserID,
					account.ID,
					account.Balance,
				)
			} else {
				app.logger.Printf(
					"Kafka user.created обработан: event_id=%s user_id=%d счёт уже существовал",
					event.EventID,
					event.UserID,
				)
			}

			break
		}

		if err := commitKafkaMessage(
			ctx,
			reader,
			message,
		); err != nil {
			if ctx.Err() != nil {
				return
			}

			app.logger.Printf(
				"Ошибка Kafka commit user.created event_id=%s: %v",
				event.EventID,
				err,
			)
		}
	}
}

// consumeOrderCreatedEvents слушает order.created.
//
// Обработка заказа выполняется транзакционно:
//
// Inbox -> SELECT FOR UPDATE -> balance -> Outbox -> COMMIT.
//
// Kafka offset подтверждается только после успешного DB commit.
func (app *Application) consumeOrderCreatedEvents(
	ctx context.Context,
) {
	if err := app.waitForKafkaTopic(
		ctx,
		orderCreatedTopic,
	); err != nil {
		if errors.Is(
			err,
			context.Canceled,
		) || ctx.Err() != nil {
			return
		}

		app.logger.Printf(
			"Не удалось дождаться Kafka topic %s: %v",
			orderCreatedTopic,
			err,
		)

		return
	}

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

			CommitInterval: 0,
		},
	)

	defer reader.Close()

	app.logger.Printf(
		"Kafka consumer запущен: topic=%s group=%s broker=%s manual_commit=true",
		orderCreatedTopic,
		billingOrderCreatedConsumerGroup,
		getKafkaBroker(),
	)

	for {
		message, err := reader.FetchMessage(
			ctx,
		)
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

			if err := commitKafkaMessage(
				ctx,
				reader,
				message,
			); err != nil {
				app.logger.Printf(
					"Ошибка commit некорректного order.created: %v",
					err,
				)
			}

			continue
		}

		if event.EventID == "" ||
			event.OrderID <= 0 ||
			event.UserID <= 0 ||
			event.Price <= 0 {
			app.logger.Printf(
				"Пропущено некорректное order.created: event_id=%q order_id=%d user_id=%d price=%d",
				event.EventID,
				event.OrderID,
				event.UserID,
				event.Price,
			)

			if err := commitKafkaMessage(
				ctx,
				reader,
				message,
			); err != nil {
				app.logger.Printf(
					"Ошибка commit некорректного order.created: %v",
					err,
				)
			}

			continue
		}

		var account Account
		var sufficientFunds bool
		var duplicate bool

		for {
			account,
				sufficientFunds,
				duplicate,
				err = app.processOrderCreatedEvent(
				ctx,
				event,
			)

			if err != nil {
				if ctx.Err() != nil {
					return
				}

				app.logger.Printf(
					"Ошибка транзакционной оплаты event_id=%s order_id=%d user_id=%d: %v",
					event.EventID,
					event.OrderID,
					event.UserID,
					err,
				)

				if !waitKafkaProcessingRetry(ctx) {
					return
				}

				continue
			}

			break
		}

		if duplicate {
			app.logger.Printf(
				"Inbox: повторное order.created пропущено event_id=%s order_id=%d",
				event.EventID,
				event.OrderID,
			)
		} else if sufficientFunds {
			app.logger.Printf(
				"Заказ успешно оплачен транзакционно: event_id=%s order_id=%d user_id=%d price=%d balance=%d outbox_topic=%s",
				event.EventID,
				event.OrderID,
				event.UserID,
				event.Price,
				account.Balance,
				paymentSucceededTopic,
			)
		} else {
			app.logger.Printf(
				"Недостаточно средств: event_id=%s order_id=%d user_id=%d price=%d balance=%d outbox_topic=%s",
				event.EventID,
				event.OrderID,
				event.UserID,
				event.Price,
				account.Balance,
				paymentFailedTopic,
			)
		}

		if err := commitKafkaMessage(
			ctx,
			reader,
			message,
		); err != nil {
			if ctx.Err() != nil {
				return
			}

			app.logger.Printf(
				"Ошибка Kafka commit order.created event_id=%s: %v",
				event.EventID,
				err,
			)
		}
	}
}

// commitKafkaMessage подтверждает offset.
//
// Вызывается только после успешной обработки события
// или для заведомо некорректного poison message.
func commitKafkaMessage(
	ctx context.Context,
	reader *kafka.Reader,
	message kafka.Message,
) error {
	return reader.CommitMessages(
		ctx,
		message,
	)
}

func waitKafkaProcessingRetry(
	ctx context.Context,
) bool {
	timer := time.NewTimer(
		kafkaProcessingRetryWait,
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
		os.Getenv(
			"KAFKA_BROKER",
		),
	)

	if broker == "" {
		return defaultKafkaBroker
	}

	return broker
}
