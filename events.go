package main

import "time"

const (
	userCreatedTopic  = "user.created"
	orderCreatedTopic = "order.created"

	defaultKafkaBroker = "localhost:9092"
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
