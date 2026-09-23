package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	errNoCourierAvailable = errors.New(
		"no courier available for requested delivery slot",
	)

	errReservationConflict = errors.New(
		"delivery reservation for this order already exists with different data",
	)

	errReservationWasReleased = errors.New(
		"delivery reservation for this order was already released",
	)
)

// Courier описывает курьера.
type Courier struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// DeliveryReservation описывает резерв курьера.
type DeliveryReservation struct {
	ID           int64     `json:"id"`
	OrderID      int64     `json:"orderId"`
	CourierID    int64     `json:"courierId"`
	DeliverySlot time.Time `json:"deliverySlot"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

// ReserveDeliveryRequest приходит из Order Service.
type ReserveDeliveryRequest struct {
	OrderID      int64  `json:"orderId"`
	DeliverySlot string `json:"deliverySlot"`
}

// reserveDeliveryHandler резервирует курьера
// на конкретный deliverySlot.
func (app *Application) reserveDeliveryHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request ReserveDeliveryRequest

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

	deliverySlot, err := time.Parse(
		time.RFC3339,
		strings.TrimSpace(request.DeliverySlot),
	)

	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"deliverySlot must be RFC3339 date-time",
		)
		return
	}

	reservation, err := app.reserveDelivery(
		r.Context(),
		request.OrderID,
		deliverySlot.UTC(),
	)

	switch {
	case errors.Is(
		err,
		errNoCourierAvailable,
	):
		writeError(
			w,
			http.StatusConflict,
			errNoCourierAvailable.Error(),
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
			"courier is already reserved for requested delivery slot",
		)
		return

	case err != nil:
		app.logger.Printf(
			"Ошибка резервирования курьера order_id=%d: %v",
			request.OrderID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to reserve courier",
		)
		return
	}

	app.logger.Printf(
		"Курьер зарезервирован order_id=%d courier_id=%d slot=%s",
		reservation.OrderID,
		reservation.CourierID,
		reservation.DeliverySlot.Format(time.RFC3339),
	)

	writeJSON(
		w,
		http.StatusCreated,
		reservation,
	)
}

// getReservationHandler возвращает резерв доставки.
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
			"delivery reservation not found",
		)
		return
	}

	if err != nil {
		app.logger.Printf(
			"Ошибка получения резерва доставки order_id=%d: %v",
			orderID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to get delivery reservation",
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		reservation,
	)
}

// releaseReservationHandler снимает резерв курьера.
//
// Для текущей Saga Delivery является последним шагом,
// однако endpoint компенсации оставляем,
// чтобы операция была полноценной и расширяемой.
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
			"Ошибка снятия резерва доставки order_id=%d: %v",
			orderID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to release delivery reservation",
		)
		return
	}

	app.logger.Printf(
		"Резерв доставки снят или уже отсутствовал order_id=%d",
		orderID,
	)

	w.WriteHeader(
		http.StatusNoContent,
	)
}

// reserveDelivery атомарно:
//
// 1. проверяет существующий резерв заказа;
// 2. ищет свободного активного курьера;
// 3. блокирует выбранного курьера;
// 4. создаёт резерв.
func (app *Application) reserveDelivery(
	parentContext context.Context,
	orderID int64,
	deliverySlot time.Time,
) (
	DeliveryReservation,
	error,
) {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	tx, err := app.db.Begin(ctx)
	if err != nil {
		return DeliveryReservation{}, err
	}

	defer tx.Rollback(ctx)

	existing, err := getReservationWithQuery(
		ctx,
		tx,
		orderID,
	)

	if err == nil {
		if existing.Status == "RELEASED" {
			return DeliveryReservation{},
				errReservationWasReleased
		}

		if !existing.DeliverySlot.Equal(
			deliverySlot,
		) {
			return DeliveryReservation{},
				errReservationConflict
		}

		// Идемпотентность:
		// одинаковый повторный запрос возвращает тот же резерв.
		return existing, nil
	}

	if !errors.Is(
		err,
		pgx.ErrNoRows,
	) {
		return DeliveryReservation{}, err
	}

	var courier Courier

	err = tx.QueryRow(
		ctx,
		`
		SELECT
			c.id,
			c.name,
			c.active,
			c.created_at,
			c.updated_at
		FROM delivery_couriers c
		WHERE
			c.active = TRUE
			AND NOT EXISTS
			(
				SELECT 1
				FROM delivery_reservations r
				WHERE
					r.courier_id = c.id
					AND r.delivery_slot = $1
					AND r.status = 'RESERVED'
			)
		ORDER BY c.id
		LIMIT 1
		FOR UPDATE OF c
		SKIP LOCKED
		`,
		deliverySlot,
	).Scan(
		&courier.ID,
		&courier.Name,
		&courier.Active,
		&courier.CreatedAt,
		&courier.UpdatedAt,
	)

	if errors.Is(
		err,
		pgx.ErrNoRows,
	) {
		return DeliveryReservation{},
			errNoCourierAvailable
	}

	if err != nil {
		return DeliveryReservation{}, err
	}

	var reservation DeliveryReservation

	err = tx.QueryRow(
		ctx,
		`
		INSERT INTO delivery_reservations
		(
			order_id,
			courier_id,
			delivery_slot,
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
			courier_id,
			delivery_slot,
			status,
			created_at,
			updated_at
		`,
		orderID,
		courier.ID,
		deliverySlot,
	).Scan(
		&reservation.ID,
		&reservation.OrderID,
		&reservation.CourierID,
		&reservation.DeliverySlot,
		&reservation.Status,
		&reservation.CreatedAt,
		&reservation.UpdatedAt,
	)

	if err != nil {
		return DeliveryReservation{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return DeliveryReservation{}, err
	}

	return reservation, nil
}

// getReservation получает резерв доставки по orderId.
func (app *Application) getReservation(
	parentContext context.Context,
	orderID int64,
) (
	DeliveryReservation,
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

// releaseReservation выполняет идемпотентную компенсацию.
//
// RESERVED -> RELEASED.
//
// После этого курьер снова считается свободным
// для данного deliverySlot.
func (app *Application) releaseReservation(
	parentContext context.Context,
	orderID int64,
) error {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	_, err := app.db.Exec(
		ctx,
		`
		UPDATE delivery_reservations
		SET
			status = 'RELEASED',
			updated_at = CURRENT_TIMESTAMP
		WHERE
			order_id = $1
			AND status = 'RESERVED'
		`,
		orderID,
	)

	return err
}

// reservationQuerier позволяет использовать общий SELECT
// с Pool и Transaction.
type reservationQuerier interface {
	QueryRow(
		context.Context,
		string,
		...any,
	) pgx.Row
}

// getReservationWithQuery выполняет общий SELECT.
func getReservationWithQuery(
	ctx context.Context,
	queryer reservationQuerier,
	orderID int64,
) (
	DeliveryReservation,
	error,
) {
	var reservation DeliveryReservation

	err := queryer.QueryRow(
		ctx,
		`
		SELECT
			id,
			order_id,
			courier_id,
			delivery_slot,
			status,
			created_at,
			updated_at
		FROM delivery_reservations
		WHERE order_id = $1
		`,
		orderID,
	).Scan(
		&reservation.ID,
		&reservation.OrderID,
		&reservation.CourierID,
		&reservation.DeliverySlot,
		&reservation.Status,
		&reservation.CreatedAt,
		&reservation.UpdatedAt,
	)

	return reservation, err
}
