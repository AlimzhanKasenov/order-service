package main

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Order описывает заказ пользователя.
type Order struct {
	ID           int64      `json:"id"`
	UserID       int64      `json:"userId"`
	Price        int64      `json:"price"`
	ProductID    *int64     `json:"productId,omitempty"`
	Quantity     *int64     `json:"quantity,omitempty"`
	DeliverySlot *time.Time `json:"deliverySlot,omitempty"`
	Status       string     `json:"status"`
	SagaError    *string    `json:"sagaError,omitempty"`
	CreatedAt    time.Time  `json:"createdAt"`
	UpdatedAt    time.Time  `json:"updatedAt"`
}

// CreateOrderRequest описывает запрос создания заказа.
//
// Старый формат Stream Processing:
//
//	{
//	  "userId": 1,
//	  "price": 3000
//	}
//
// Новый формат Saga:
//
//	{
//	  "userId": 1,
//	  "price": 3000,
//	  "productId": 1,
//	  "quantity": 2,
//	  "deliverySlot": "2026-09-23T10:00:00Z"
//	}
type CreateOrderRequest struct {
	UserID       int64  `json:"userId"`
	Price        int64  `json:"price"`
	ProductID    int64  `json:"productId"`
	Quantity     int64  `json:"quantity"`
	DeliverySlot string `json:"deliverySlot"`
}

// createOrderHandler создаёт заказ.
//
// Старый запрос без productId, quantity и deliverySlot
// продолжает работать через Kafka.
//
// Новый запрос с данными товара и доставки
// запускает распределённую транзакцию Saga.
func (app *Application) createOrderHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request CreateOrderRequest

	if err := decodeJSON(
		w,
		r,
		&request,
	); err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			err.Error(),
		)
		return
	}

	if request.UserID <= 0 {
		writeError(
			w,
			http.StatusBadRequest,
			"userId must be a positive integer",
		)
		return
	}

	if request.Price <= 0 {
		writeError(
			w,
			http.StatusBadRequest,
			"price must be greater than zero",
		)
		return
	}

	isSagaRequest :=
		request.ProductID != 0 ||
			request.Quantity != 0 ||
			strings.TrimSpace(request.DeliverySlot) != ""

	if isSagaRequest {
		app.createSagaOrderHandler(
			w,
			r,
			request,
		)
		return
	}

	app.createLegacyOrderHandler(
		w,
		r,
		request,
	)
}

// createLegacyOrderHandler сохраняет поведение
// предыдущего домашнего задания Stream Processing.
func (app *Application) createLegacyOrderHandler(
	w http.ResponseWriter,
	r *http.Request,
	request CreateOrderRequest,
) {
	order, _, err := app.createOrder(
		r.Context(),
		request.UserID,
		request.Price,
	)

	if errors.Is(
		err,
		pgx.ErrNoRows,
	) {
		writeError(
			w,
			http.StatusNotFound,
			"user not found",
		)
		return
	}

	if err != nil {
		app.logger.Printf(
			"Ошибка создания заказа: %v",
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to create order",
		)
		return
	}

	emitStructuredLog(
		"INFO",
		"Заказ создан",
		map[string]any{
			"event":    "order_created",
			"order_id": order.ID,
			"user_id":  order.UserID,
			"price":    order.Price,
			"status":   order.Status,
		},
	)

	emitStructuredLog(
		"INFO",
		"Событие order.created сохранено в Transactional Outbox",
		map[string]any{
			"event":    "order_created_outbox",
			"order_id": order.ID,
			"user_id":  order.UserID,
			"topic":    orderCreatedTopic,
		},
	)

	writeJSON(
		w,
		http.StatusCreated,
		order,
	)
}

// getOrderHandler возвращает заказ по ID.
func (app *Application) getOrderHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	orderID, err := parseOrderID(r)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			err.Error(),
		)
		return
	}

	order, err := app.getOrder(
		r.Context(),
		orderID,
	)

	if errors.Is(
		err,
		pgx.ErrNoRows,
	) {
		writeError(
			w,
			http.StatusNotFound,
			"order not found",
		)
		return
	}

	if err != nil {
		app.logger.Printf(
			"Ошибка получения заказа ID %d: %v",
			orderID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to get order",
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		order,
	)
}

// createOrder создаёт обычный заказ предыдущего Stream Processing
// и одновременно получает email пользователя для события order.created.
func (app *Application) createOrder(
	parentContext context.Context,
	userID int64,
	price int64,
) (
	Order,
	string,
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
		return Order{}, "", err
	}

	defer tx.Rollback(ctx)

	var email string

	err = tx.QueryRow(
		ctx,
		`
		SELECT email
		FROM users
		WHERE id = $1
		`,
		userID,
	).Scan(
		&email,
	)

	if err != nil {
		return Order{}, "", err
	}

	var order Order

	err = tx.QueryRow(
		ctx,
		`
		INSERT INTO orders
		(
			user_id,
			price,
			status
		)
		VALUES ($1, $2, 'NEW')
		RETURNING
			id,
			user_id,
			price,
			status,
			created_at,
			updated_at
		`,
		userID,
		price,
	).Scan(
		&order.ID,
		&order.UserID,
		&order.Price,
		&order.Status,
		&order.CreatedAt,
		&order.UpdatedAt,
	)

	if err != nil {
		return Order{}, "", err
	}

	event := OrderCreatedEvent{
		OrderID:   order.ID,
		UserID:    order.UserID,
		Price:     order.Price,
		Email:     email,
		CreatedAt: time.Now().UTC(),
	}

	_, err = insertOutboxEventTx(
		ctx,
		tx,
		orderCreatedTopic,
		strconv.FormatInt(
			order.ID,
			10,
		),
		event,
	)

	if err != nil {
		return Order{}, "", err
	}

	if err := tx.Commit(ctx); err != nil {
		return Order{}, "", err
	}

	return order, email, nil
}

// getOrder получает заказ по ID.
func (app *Application) getOrder(
	parentContext context.Context,
	orderID int64,
) (
	Order,
	error,
) {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	var order Order

	err := app.db.QueryRow(
		ctx,
		`
		SELECT
			id,
			user_id,
			price,
			product_id,
			quantity,
			delivery_slot,
			status,
			saga_error,
			created_at,
			updated_at
		FROM orders
		WHERE id = $1
		`,
		orderID,
	).Scan(
		&order.ID,
		&order.UserID,
		&order.Price,
		&order.ProductID,
		&order.Quantity,
		&order.DeliverySlot,
		&order.Status,
		&order.SagaError,
		&order.CreatedAt,
		&order.UpdatedAt,
	)

	return order, err
}

// parseOrderID получает ID заказа из URL.
func parseOrderID(
	r *http.Request,
) (
	int64,
	error,
) {
	orderID, err := strconv.ParseInt(
		r.PathValue("orderId"),
		10,
		64,
	)

	if err != nil || orderID <= 0 {
		return 0, errors.New(
			"orderId must be a positive integer",
		)
	}

	return orderID, nil
}
