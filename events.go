package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	userCreatedTopic  = "user.created"
	orderCreatedTopic = "order.created"

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

// publishUserCreatedEvent публикует событие user.created.
func publishUserCreatedEvent(
	parentContext context.Context,
	user User,
) error {
	event := UserCreatedEvent{
		UserID:    user.ID,
		Username:  user.Username,
		Email:     user.Email,
		CreatedAt: time.Now().UTC(),
	}

	return publishKafkaEvent(
		parentContext,
		userCreatedTopic,
		strconv.FormatInt(
			user.ID,
			10,
		),
		event,
	)
}

// publishOrderCreatedEvent публикует событие order.created.
//
// Billing Service получает это событие,
// проверяет баланс и выполняет списание.
func publishOrderCreatedEvent(
	parentContext context.Context,
	order Order,
	email string,
) error {
	event := OrderCreatedEvent{
		OrderID:   order.ID,
		UserID:    order.UserID,
		Price:     order.Price,
		Email:     email,
		CreatedAt: time.Now().UTC(),
	}

	return publishKafkaEvent(
		parentContext,
		orderCreatedTopic,
		strconv.FormatInt(
			order.ID,
			10,
		),
		event,
	)
}

// publishKafkaEvent выполняет общую публикацию JSON-события.
func publishKafkaEvent(
	parentContext context.Context,
	topic string,
	key string,
	event any,
) error {
	payload, err := json.Marshal(event)

	if err != nil {
		return fmt.Errorf(
			"failed to encode Kafka event: %w",
			err,
		)
	}

	broker := strings.TrimSpace(
		os.Getenv("KAFKA_BROKER"),
	)

	if broker == "" {
		broker = defaultKafkaBroker
	}

	writer := &kafka.Writer{
		Addr: kafka.TCP(
			broker,
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
				key,
			),

			Value: payload,

			Time: time.Now().UTC(),
		},
	)

	if err != nil {
		return fmt.Errorf(
			"failed to publish topic %s: %w",
			topic,
			err,
		)
	}

	return nil
}
