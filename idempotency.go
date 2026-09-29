package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrIdempotencyConflict означает, что один и тот же
// Idempotency-Key был использован для разных запросов.
var ErrIdempotencyConflict = errors.New(
	"idempotency key was already used with another request",
)

// normalizeIdempotencyKey проверяет значение HTTP-заголовка
// Idempotency-Key.
//
// Пустой ключ разрешён для обратной совместимости:
// старые тесты и предыдущие домашние задания продолжают работать.
func normalizeIdempotencyKey(
	value string,
) (
	string,
	error,
) {
	key := strings.TrimSpace(value)

	if key == "" {
		return "", nil
	}

	if len(key) > 128 {
		return "", errors.New(
			"Idempotency-Key must not exceed 128 characters",
		)
	}

	return key, nil
}

// hashSagaCreateOrderRequest вычисляет SHA-256 от бизнес-данных
// запроса создания Saga-заказа.
//
// Благодаря request_hash нельзя случайно использовать один
// Idempotency-Key для двух разных заказов.
func hashSagaCreateOrderRequest(
	request CreateOrderRequest,
	deliverySlot time.Time,
) (
	string,
	error,
) {
	canonicalRequest := struct {
		UserID       int64  `json:"userId"`
		Price        int64  `json:"price"`
		ProductID    int64  `json:"productId"`
		Quantity     int64  `json:"quantity"`
		DeliverySlot string `json:"deliverySlot"`
	}{
		UserID:       request.UserID,
		Price:        request.Price,
		ProductID:    request.ProductID,
		Quantity:     request.Quantity,
		DeliverySlot: deliverySlot.UTC().Format(time.RFC3339Nano),
	}

	payload, err := json.Marshal(
		canonicalRequest,
	)
	if err != nil {
		return "", fmt.Errorf(
			"failed to build idempotency request hash: %w",
			err,
		)
	}

	sum := sha256.Sum256(payload)

	return hex.EncodeToString(
		sum[:],
	), nil
}

// createSagaOrderIdempotently создаёт Saga-заказ с поддержкой
// паттерна Idempotency Key.
//
// Если Idempotency-Key пустой, используется старое поведение.
//
// Если ключ уже существует и request_hash совпадает,
// возвращается ранее созданный заказ и replayed=true.
//
// Если ключ существует, но request_hash отличается,
// возвращается ErrIdempotencyConflict.
//
// Создание заказа и сохранение Idempotency-Key выполняются
// в одной транзакции PostgreSQL. UNIQUE PRIMARY KEY защищает
// от одновременного создания двух заказов с одним ключом.
func (app *Application) createSagaOrderIdempotently(
	parentContext context.Context,
	request CreateOrderRequest,
	deliverySlot time.Time,
	idempotencyKey string,
	requestHash string,
) (
	Order,
	bool,
	error,
) {
	if idempotencyKey == "" {
		order, err := app.createSagaOrder(
			parentContext,
			request,
			deliverySlot,
		)

		return order, false, err
	}

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
		return Order{}, false, err
	}

	defer tx.Rollback(ctx)

	// Сначала проверяем обычный повтор уже завершённого запроса.
	existingOrder, existingHash, err :=
		getIdempotentOrderTx(
			ctx,
			tx,
			idempotencyKey,
		)

	if err == nil {
		if existingHash != requestHash {
			return Order{},
				false,
				fmt.Errorf(
					"%w: %s",
					ErrIdempotencyConflict,
					idempotencyKey,
				)
		}

		return existingOrder, true, nil
	}

	if !errors.Is(
		err,
		pgx.ErrNoRows,
	) {
		return Order{}, false, err
	}

	// Проверяем существование пользователя.
	var existingUserID int64

	err = tx.QueryRow(
		ctx,
		`
		SELECT id
		FROM users
		WHERE id = $1
		`,
		request.UserID,
	).Scan(
		&existingUserID,
	)

	if err != nil {
		return Order{}, false, err
	}

	// Создаём локальную запись заказа.
	var order Order

	err = tx.QueryRow(
		ctx,
		`
		INSERT INTO orders
		(
			user_id,
			price,
			product_id,
			quantity,
			delivery_slot,
			status,
			saga_error
		)
		VALUES
		(
			$1,
			$2,
			$3,
			$4,
			$5,
			$6,
			NULL
		)
		RETURNING
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
		`,
		request.UserID,
		request.Price,
		request.ProductID,
		request.Quantity,
		deliverySlot.UTC(),
		orderStatusSagaStarted,
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

	if err != nil {
		return Order{}, false, err
	}

	// Пытаемся занять Idempotency-Key.
	//
	// При параллельных одинаковых запросах PostgreSQL
	// гарантирует, что ключ получит только один из них.
	var insertedKey string

	err = tx.QueryRow(
		ctx,
		`
		INSERT INTO order_idempotency_keys
		(
			idempotency_key,
			request_hash,
			order_id
		)
		VALUES ($1, $2, $3)
		ON CONFLICT (idempotency_key)
		DO NOTHING
		RETURNING idempotency_key
		`,
		idempotencyKey,
		requestHash,
		order.ID,
	).Scan(
		&insertedKey,
	)

	if errors.Is(
		err,
		pgx.ErrNoRows,
	) {
		// Другой параллельный запрос успел сохранить этот ключ.
		//
		// Наш только что созданный order остаётся внутри текущей
		// транзакции и будет автоматически откатан через Rollback.
		existingOrder, existingHash, readErr :=
			getIdempotentOrderTx(
				ctx,
				tx,
				idempotencyKey,
			)

		if readErr != nil {
			return Order{},
				false,
				fmt.Errorf(
					"failed to read concurrent idempotency request: %w",
					readErr,
				)
		}

		if existingHash != requestHash {
			return Order{},
				false,
				fmt.Errorf(
					"%w: %s",
					ErrIdempotencyConflict,
					idempotencyKey,
				)
		}

		return existingOrder, true, nil
	}

	if err != nil {
		return Order{}, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Order{}, false, err
	}

	return order, false, nil
}

// getIdempotentOrderTx получает заказ,
// связанный с Idempotency-Key.
func getIdempotentOrderTx(
	ctx context.Context,
	tx pgx.Tx,
	idempotencyKey string,
) (
	Order,
	string,
	error,
) {
	var (
		order       Order
		requestHash string
	)

	err := tx.QueryRow(
		ctx,
		`
		SELECT
			i.request_hash,
			o.id,
			o.user_id,
			o.price,
			o.product_id,
			o.quantity,
			o.delivery_slot,
			o.status,
			o.saga_error,
			o.created_at,
			o.updated_at
		FROM order_idempotency_keys AS i
		INNER JOIN orders AS o
			ON o.id = i.order_id
		WHERE i.idempotency_key = $1
		`,
		idempotencyKey,
	).Scan(
		&requestHash,
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

	return order, requestHash, err
}
