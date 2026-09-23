package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	errInsufficientStock = errors.New(
		"insufficient stock",
	)

	errReservationConflict = errors.New(
		"reservation for this order already exists with different data",
	)

	errReservationWasReleased = errors.New(
		"reservation for this order was already released",
	)
)

// Product описывает товар на складе.
type Product struct {
	ID                int64     `json:"id"`
	Name              string    `json:"name"`
	AvailableQuantity int64     `json:"availableQuantity"`
	CreatedAt         time.Time `json:"createdAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
}

// Reservation описывает резерв товара
// для конкретного заказа.
type Reservation struct {
	ID        int64     `json:"id"`
	OrderID   int64     `json:"orderId"`
	ProductID int64     `json:"productId"`
	Quantity  int64     `json:"quantity"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// ReserveInventoryRequest описывает запрос
// от Order Service на резервирование товара.
type ReserveInventoryRequest struct {
	OrderID   int64 `json:"orderId"`
	ProductID int64 `json:"productId"`
	Quantity  int64 `json:"quantity"`
}

// AddStockRequest используется
// для пополнения остатка товара.
type AddStockRequest struct {
	Quantity int64 `json:"quantity"`
}

// getProductHandler возвращает товар и текущий остаток.
func (app *Application) getProductHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	productID, err := parsePositivePathID(
		r,
		"productId",
	)

	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			err.Error(),
		)
		return
	}

	product, err := app.getProduct(
		r.Context(),
		productID,
	)

	if errors.Is(
		err,
		pgx.ErrNoRows,
	) {
		writeError(
			w,
			http.StatusNotFound,
			"product not found",
		)
		return
	}

	if err != nil {
		app.logger.Printf(
			"Ошибка получения товара %d: %v",
			productID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to get product",
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		product,
	)
}

// addStockHandler пополняет остаток товара.
func (app *Application) addStockHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	productID, err := parsePositivePathID(
		r,
		"productId",
	)

	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			err.Error(),
		)
		return
	}

	var request AddStockRequest

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

	if request.Quantity <= 0 {
		writeError(
			w,
			http.StatusBadRequest,
			"quantity must be greater than zero",
		)
		return
	}

	product, err := app.addStock(
		r.Context(),
		productID,
		request.Quantity,
	)

	if errors.Is(
		err,
		pgx.ErrNoRows,
	) {
		writeError(
			w,
			http.StatusNotFound,
			"product not found",
		)
		return
	}

	if err != nil {
		app.logger.Printf(
			"Ошибка пополнения товара %d: %v",
			productID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to add stock",
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		product,
	)
}

// reserveInventoryHandler резервирует товар.
//
// Этот endpoint вызывается Order Service
// на втором шаге Saga.
func (app *Application) reserveInventoryHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request ReserveInventoryRequest

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

	if request.OrderID <= 0 {
		writeError(
			w,
			http.StatusBadRequest,
			"orderId must be a positive integer",
		)
		return
	}

	if request.ProductID <= 0 {
		writeError(
			w,
			http.StatusBadRequest,
			"productId must be a positive integer",
		)
		return
	}

	if request.Quantity <= 0 {
		writeError(
			w,
			http.StatusBadRequest,
			"quantity must be greater than zero",
		)
		return
	}

	reservation, err := app.reserveInventory(
		r.Context(),
		request,
	)

	switch {
	case errors.Is(
		err,
		pgx.ErrNoRows,
	):
		writeError(
			w,
			http.StatusNotFound,
			"product not found",
		)
		return

	case errors.Is(
		err,
		errInsufficientStock,
	):
		writeError(
			w,
			http.StatusConflict,
			"insufficient stock",
		)
		return

	case errors.Is(
		err,
		errReservationConflict,
	):
		writeError(
			w,
			http.StatusConflict,
			errReservationConflict.Error(),
		)
		return

	case errors.Is(
		err,
		errReservationWasReleased,
	):
		writeError(
			w,
			http.StatusConflict,
			errReservationWasReleased.Error(),
		)
		return

	case err != nil && isUniqueViolation(err):
		writeError(
			w,
			http.StatusConflict,
			"reservation for this order already exists",
		)
		return

	case err != nil:
		app.logger.Printf(
			"Ошибка резервирования товара order_id=%d: %v",
			request.OrderID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to reserve inventory",
		)
		return
	}

	app.logger.Printf(
		"Товар зарезервирован order_id=%d product_id=%d quantity=%d",
		reservation.OrderID,
		reservation.ProductID,
		reservation.Quantity,
	)

	writeJSON(
		w,
		http.StatusCreated,
		reservation,
	)
}

// getReservationHandler возвращает резерв по orderId.
func (app *Application) getReservationHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	orderID, err := parsePositivePathID(
		r,
		"orderId",
	)

	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			err.Error(),
		)
		return
	}

	reservation, err := app.getReservation(
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
			"reservation not found",
		)
		return
	}

	if err != nil {
		app.logger.Printf(
			"Ошибка получения резерва order_id=%d: %v",
			orderID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to get reservation",
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		reservation,
	)
}

// releaseReservationHandler снимает резерв.
//
// Это компенсирующая транзакция Saga.
//
// Повторный вызов безопасен:
// если резерв уже снят или отсутствует,
// endpoint всё равно вернёт 204.
func (app *Application) releaseReservationHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	orderID, err := parsePositivePathID(
		r,
		"orderId",
	)

	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			err.Error(),
		)
		return
	}

	if err := app.releaseReservation(
		r.Context(),
		orderID,
	); err != nil {
		app.logger.Printf(
			"Ошибка снятия резерва order_id=%d: %v",
			orderID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to release reservation",
		)
		return
	}

	app.logger.Printf(
		"Резерв снят или уже отсутствовал order_id=%d",
		orderID,
	)

	w.WriteHeader(
		http.StatusNoContent,
	)
}

// getProduct получает товар.
func (app *Application) getProduct(
	parentContext context.Context,
	productID int64,
) (
	Product,
	error,
) {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	var product Product

	err := app.db.QueryRow(
		ctx,
		`
		SELECT
			id,
			name,
			available_quantity,
			created_at,
			updated_at
		FROM inventory_products
		WHERE id = $1
		`,
		productID,
	).Scan(
		&product.ID,
		&product.Name,
		&product.AvailableQuantity,
		&product.CreatedAt,
		&product.UpdatedAt,
	)

	return product, err
}

// addStock атомарно пополняет остаток.
func (app *Application) addStock(
	parentContext context.Context,
	productID int64,
	quantity int64,
) (
	Product,
	error,
) {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	var product Product

	err := app.db.QueryRow(
		ctx,
		`
		UPDATE inventory_products
		SET
			available_quantity = available_quantity + $2,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		RETURNING
			id,
			name,
			available_quantity,
			created_at,
			updated_at
		`,
		productID,
		quantity,
	).Scan(
		&product.ID,
		&product.Name,
		&product.AvailableQuantity,
		&product.CreatedAt,
		&product.UpdatedAt,
	)

	return product, err
}

// reserveInventory атомарно:
//
// 1. блокирует строку товара;
// 2. проверяет остаток;
// 3. уменьшает остаток;
// 4. создаёт резерв.
//
// Если любой шаг завершится ошибкой,
// транзакция PostgreSQL откатится.
func (app *Application) reserveInventory(
	parentContext context.Context,
	request ReserveInventoryRequest,
) (
	Reservation,
	error,
) {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	tx, err := app.db.Begin(ctx)
	if err != nil {
		return Reservation{}, err
	}

	defer tx.Rollback(ctx)

	existing, err := getReservationWithQuery(
		ctx,
		tx,
		request.OrderID,
	)

	if err == nil {
		if existing.Status == "RELEASED" {
			return Reservation{},
				errReservationWasReleased
		}

		if existing.ProductID != request.ProductID ||
			existing.Quantity != request.Quantity {
			return Reservation{},
				errReservationConflict
		}

		// Идемпотентность:
		// повторный одинаковый запрос не уменьшает остаток ещё раз.
		return existing, nil
	}

	if !errors.Is(
		err,
		pgx.ErrNoRows,
	) {
		return Reservation{}, err
	}

	var availableQuantity int64

	err = tx.QueryRow(
		ctx,
		`
		SELECT available_quantity
		FROM inventory_products
		WHERE id = $1
		FOR UPDATE
		`,
		request.ProductID,
	).Scan(
		&availableQuantity,
	)

	if err != nil {
		return Reservation{}, err
	}

	if availableQuantity < request.Quantity {
		return Reservation{},
			errInsufficientStock
	}

	_, err = tx.Exec(
		ctx,
		`
		UPDATE inventory_products
		SET
			available_quantity = available_quantity - $2,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		`,
		request.ProductID,
		request.Quantity,
	)

	if err != nil {
		return Reservation{}, err
	}

	var reservation Reservation

	err = tx.QueryRow(
		ctx,
		`
		INSERT INTO inventory_reservations
		(
			order_id,
			product_id,
			quantity,
			status
		)
		VALUES
		(
			$1,
			$2,
			$3,
			'RESERVED'
		)
		RETURNING
			id,
			order_id,
			product_id,
			quantity,
			status,
			created_at,
			updated_at
		`,
		request.OrderID,
		request.ProductID,
		request.Quantity,
	).Scan(
		&reservation.ID,
		&reservation.OrderID,
		&reservation.ProductID,
		&reservation.Quantity,
		&reservation.Status,
		&reservation.CreatedAt,
		&reservation.UpdatedAt,
	)

	if err != nil {
		return Reservation{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Reservation{}, err
	}

	return reservation, nil
}

// getReservation получает резерв по orderId.
func (app *Application) getReservation(
	parentContext context.Context,
	orderID int64,
) (
	Reservation,
	error,
) {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	return getReservationWithQuery(
		ctx,
		app.db,
		orderID,
	)
}

// releaseReservation выполняет компенсацию:
//
// RESERVED -> RELEASED
//
// и возвращает количество товара обратно на склад.
//
// Операция идемпотентна:
// повторный вызов не увеличивает остаток повторно.
func (app *Application) releaseReservation(
	parentContext context.Context,
	orderID int64,
) error {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	tx, err := app.db.Begin(ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback(ctx)

	var reservation Reservation

	err = tx.QueryRow(
		ctx,
		`
		SELECT
			id,
			order_id,
			product_id,
			quantity,
			status,
			created_at,
			updated_at
		FROM inventory_reservations
		WHERE order_id = $1
		FOR UPDATE
		`,
		orderID,
	).Scan(
		&reservation.ID,
		&reservation.OrderID,
		&reservation.ProductID,
		&reservation.Quantity,
		&reservation.Status,
		&reservation.CreatedAt,
		&reservation.UpdatedAt,
	)

	if errors.Is(
		err,
		pgx.ErrNoRows,
	) {
		return nil
	}

	if err != nil {
		return err
	}

	if reservation.Status == "RELEASED" {
		return nil
	}

	if reservation.Status != "RESERVED" {
		return fmt.Errorf(
			"unsupported reservation status: %s",
			reservation.Status,
		)
	}

	_, err = tx.Exec(
		ctx,
		`
		UPDATE inventory_products
		SET
			available_quantity = available_quantity + $2,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		`,
		reservation.ProductID,
		reservation.Quantity,
	)

	if err != nil {
		return err
	}

	_, err = tx.Exec(
		ctx,
		`
		UPDATE inventory_reservations
		SET
			status = 'RELEASED',
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		`,
		reservation.ID,
	)

	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// reservationQuerier позволяет одной функции
// работать как с pgxpool.Pool,
// так и с PostgreSQL transaction.
type reservationQuerier interface {
	QueryRow(
		context.Context,
		string,
		...any,
	) pgx.Row
}

// getReservationWithQuery выполняет общий SELECT резерва.
func getReservationWithQuery(
	ctx context.Context,
	queryer reservationQuerier,
	orderID int64,
) (
	Reservation,
	error,
) {
	var reservation Reservation

	err := queryer.QueryRow(
		ctx,
		`
		SELECT
			id,
			order_id,
			product_id,
			quantity,
			status,
			created_at,
			updated_at
		FROM inventory_reservations
		WHERE order_id = $1
		`,
		orderID,
	).Scan(
		&reservation.ID,
		&reservation.OrderID,
		&reservation.ProductID,
		&reservation.Quantity,
		&reservation.Status,
		&reservation.CreatedAt,
		&reservation.UpdatedAt,
	)

	return reservation, err
}
