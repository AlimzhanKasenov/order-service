package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	defaultBillingServiceURL   = "http://localhost:8001"
	defaultInventoryServiceURL = "http://localhost:8003"
	defaultDeliveryServiceURL  = "http://localhost:8004"

	sagaHTTPTimeout = 5 * time.Second
)

const (
	orderStatusSagaStarted        = "SAGA_STARTED"
	orderStatusConfirmed          = "CONFIRMED"
	orderStatusSagaFailed         = "SAGA_FAILED"
	orderStatusCompensationFailed = "COMPENSATION_FAILED"
)

// SagaErrorResponse возвращается клиенту,
// когда один из шагов Saga завершился ошибкой.
type SagaErrorResponse struct {
	Message string `json:"message"`
	Order   Order  `json:"order"`
}

// InventoryReservationRequest описывает резерв товара.
type InventoryReservationRequest struct {
	OrderID   int64 `json:"orderId"`
	ProductID int64 `json:"productId"`
	Quantity  int64 `json:"quantity"`
}

// DeliveryReservationRequest описывает резерв курьера.
type DeliveryReservationRequest struct {
	OrderID      int64  `json:"orderId"`
	DeliverySlot string `json:"deliverySlot"`
}

// MoneySagaRequest используется при обращении к Billing Service.
type MoneySagaRequest struct {
	Amount int64 `json:"amount"`
}

// createSagaOrderHandler выполняет распределённую транзакцию
// по паттерну Saga Orchestration.
func (app *Application) createSagaOrderHandler(
	w http.ResponseWriter,
	r *http.Request,
	request CreateOrderRequest,
) {
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

	order, err := app.createSagaOrder(
		r.Context(),
		request,
		deliverySlot,
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
			"Ошибка создания Saga-заказа: %v",
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to create saga order",
		)
		return
	}

	emitStructuredLog(
		"INFO",
		"Saga запущена",
		map[string]any{
			"event":      "saga_started",
			"order_id":   order.ID,
			"user_id":    order.UserID,
			"product_id": request.ProductID,
			"quantity":   request.Quantity,
			"price":      request.Price,
		},
	)

	// Шаг 1. Списываем деньги.
	if err := withdrawMoney(
		r.Context(),
		request.UserID,
		request.Price,
	); err != nil {
		message := fmt.Sprintf(
			"billing step failed: %v",
			err,
		)

		app.finishSagaWithError(
			r.Context(),
			order.ID,
			orderStatusSagaFailed,
			message,
		)

		app.writeSagaFailure(
			w,
			r.Context(),
			order.ID,
			message,
			http.StatusConflict,
		)

		return
	}

	emitStructuredLog(
		"INFO",
		"Saga: оплата выполнена",
		map[string]any{
			"event":    "saga_billing_completed",
			"order_id": order.ID,
			"user_id":  request.UserID,
			"amount":   request.Price,
		},
	)

	// Шаг 2. Резервируем товар.
	inventoryRequest := InventoryReservationRequest{
		OrderID:   order.ID,
		ProductID: request.ProductID,
		Quantity:  request.Quantity,
	}

	if err := reserveInventory(
		r.Context(),
		inventoryRequest,
	); err != nil {
		originalError := fmt.Sprintf(
			"inventory step failed: %v",
			err,
		)

		compensationError := refundMoney(
			r.Context(),
			request.UserID,
			request.Price,
		)

		if compensationError != nil {
			message := fmt.Sprintf(
				"%s; billing compensation failed: %v",
				originalError,
				compensationError,
			)

			app.finishSagaWithError(
				r.Context(),
				order.ID,
				orderStatusCompensationFailed,
				message,
			)

			app.writeSagaFailure(
				w,
				r.Context(),
				order.ID,
				message,
				http.StatusInternalServerError,
			)

			return
		}

		emitStructuredLog(
			"WARN",
			"Saga: выполнена компенсация Billing",
			map[string]any{
				"event":    "saga_billing_compensated",
				"order_id": order.ID,
				"user_id":  request.UserID,
				"amount":   request.Price,
			},
		)

		app.finishSagaWithError(
			r.Context(),
			order.ID,
			orderStatusSagaFailed,
			originalError,
		)

		app.writeSagaFailure(
			w,
			r.Context(),
			order.ID,
			originalError,
			http.StatusConflict,
		)

		return
	}

	emitStructuredLog(
		"INFO",
		"Saga: товар зарезервирован",
		map[string]any{
			"event":      "saga_inventory_completed",
			"order_id":   order.ID,
			"product_id": request.ProductID,
			"quantity":   request.Quantity,
		},
	)

	// Шаг 3. Резервируем курьера.
	deliveryRequest := DeliveryReservationRequest{
		OrderID:      order.ID,
		DeliverySlot: deliverySlot.UTC().Format(time.RFC3339),
	}

	if err := reserveDelivery(
		r.Context(),
		deliveryRequest,
	); err != nil {
		originalError := fmt.Sprintf(
			"delivery step failed: %v",
			err,
		)

		compensationErrors := make(
			[]string,
			0,
		)

		// Сначала снимаем резерв товара.
		if compensationError := releaseInventory(
			r.Context(),
			order.ID,
		); compensationError != nil {
			compensationErrors = append(
				compensationErrors,
				fmt.Sprintf(
					"inventory compensation failed: %v",
					compensationError,
				),
			)
		} else {
			emitStructuredLog(
				"WARN",
				"Saga: снят резерв товара",
				map[string]any{
					"event":    "saga_inventory_compensated",
					"order_id": order.ID,
				},
			)
		}

		// Затем возвращаем деньги.
		if compensationError := refundMoney(
			r.Context(),
			request.UserID,
			request.Price,
		); compensationError != nil {
			compensationErrors = append(
				compensationErrors,
				fmt.Sprintf(
					"billing compensation failed: %v",
					compensationError,
				),
			)
		} else {
			emitStructuredLog(
				"WARN",
				"Saga: возвращены деньги",
				map[string]any{
					"event":    "saga_billing_compensated",
					"order_id": order.ID,
					"user_id":  request.UserID,
					"amount":   request.Price,
				},
			)
		}

		if len(compensationErrors) > 0 {
			message := originalError +
				"; " +
				strings.Join(
					compensationErrors,
					"; ",
				)

			app.finishSagaWithError(
				r.Context(),
				order.ID,
				orderStatusCompensationFailed,
				message,
			)

			app.writeSagaFailure(
				w,
				r.Context(),
				order.ID,
				message,
				http.StatusInternalServerError,
			)

			return
		}

		app.finishSagaWithError(
			r.Context(),
			order.ID,
			orderStatusSagaFailed,
			originalError,
		)

		app.writeSagaFailure(
			w,
			r.Context(),
			order.ID,
			originalError,
			http.StatusConflict,
		)

		return
	}

	// Все три шага завершены успешно.
	if err := app.updateSagaOrderStatus(
		r.Context(),
		order.ID,
		orderStatusConfirmed,
		"",
	); err != nil {
		app.logger.Printf(
			"Не удалось установить CONFIRMED для заказа %d: %v",
			order.ID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to finalize saga",
		)

		return
	}

	order, err = app.getOrder(
		r.Context(),
		order.ID,
	)
	if err != nil {
		writeError(
			w,
			http.StatusInternalServerError,
			"failed to read confirmed order",
		)
		return
	}

	emitStructuredLog(
		"INFO",
		"Saga успешно завершена",
		map[string]any{
			"event":    "saga_completed",
			"order_id": order.ID,
			"status":   order.Status,
		},
	)

	writeJSON(
		w,
		http.StatusCreated,
		order,
	)
}

// createSagaOrder создаёт локальную запись заказа
// перед запуском распределённой транзакции.
func (app *Application) createSagaOrder(
	parentContext context.Context,
	request CreateOrderRequest,
	deliverySlot time.Time,
) (
	Order,
	error,
) {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	tx, err := app.db.Begin(ctx)
	if err != nil {
		return Order{}, err
	}

	defer tx.Rollback(ctx)

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
		return Order{}, err
	}

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
		return Order{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Order{}, err
	}

	return order, nil
}

// updateSagaOrderStatus меняет состояние Saga-заказа.
func (app *Application) updateSagaOrderStatus(
	parentContext context.Context,
	orderID int64,
	status string,
	errorMessage string,
) error {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	var sagaError any

	if strings.TrimSpace(errorMessage) != "" {
		sagaError = errorMessage
	}

	_, err := app.db.Exec(
		ctx,
		`
		UPDATE orders
		SET
			status = $2,
			saga_error = $3,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		`,
		orderID,
		status,
		sagaError,
	)

	return err
}

// finishSagaWithError сохраняет ошибку Saga в заказе.
func (app *Application) finishSagaWithError(
	parentContext context.Context,
	orderID int64,
	status string,
	message string,
) {
	if err := app.updateSagaOrderStatus(
		parentContext,
		orderID,
		status,
		message,
	); err != nil {
		app.logger.Printf(
			"Не удалось сохранить ошибку Saga order_id=%d: %v",
			orderID,
			err,
		)
	}

	emitStructuredLog(
		"ERROR",
		"Saga завершена ошибкой",
		map[string]any{
			"event":    "saga_failed",
			"order_id": orderID,
			"status":   status,
			"error":    message,
		},
	)
}

// writeSagaFailure возвращает клиенту заказ
// вместе с причиной отката.
func (app *Application) writeSagaFailure(
	w http.ResponseWriter,
	parentContext context.Context,
	orderID int64,
	message string,
	httpStatus int,
) {
	order, err := app.getOrder(
		parentContext,
		orderID,
	)

	if err != nil {
		writeError(
			w,
			http.StatusInternalServerError,
			message,
		)
		return
	}

	writeJSON(
		w,
		httpStatus,
		SagaErrorResponse{
			Message: message,
			Order:   order,
		},
	)
}

// withdrawMoney выполняет шаг Billing:
// списывает стоимость заказа.
func withdrawMoney(
	parentContext context.Context,
	userID int64,
	amount int64,
) error {
	url := fmt.Sprintf(
		"%s/billing/accounts/%d/withdraw",
		serviceURL(
			"BILLING_SERVICE_URL",
			defaultBillingServiceURL,
		),
		userID,
	)

	return callSagaService(
		parentContext,
		http.MethodPost,
		url,
		MoneySagaRequest{
			Amount: amount,
		},
	)
}

// refundMoney выполняет компенсацию Billing:
// возвращает ранее списанные деньги.
func refundMoney(
	parentContext context.Context,
	userID int64,
	amount int64,
) error {
	url := fmt.Sprintf(
		"%s/billing/accounts/%d/deposit",
		serviceURL(
			"BILLING_SERVICE_URL",
			defaultBillingServiceURL,
		),
		userID,
	)

	return callSagaService(
		parentContext,
		http.MethodPost,
		url,
		MoneySagaRequest{
			Amount: amount,
		},
	)
}

// reserveInventory резервирует товар.
func reserveInventory(
	parentContext context.Context,
	request InventoryReservationRequest,
) error {
	url := fmt.Sprintf(
		"%s/inventory/reservations",
		serviceURL(
			"INVENTORY_SERVICE_URL",
			defaultInventoryServiceURL,
		),
	)

	return callSagaService(
		parentContext,
		http.MethodPost,
		url,
		request,
	)
}

// releaseInventory снимает резерв товара.
func releaseInventory(
	parentContext context.Context,
	orderID int64,
) error {
	url := fmt.Sprintf(
		"%s/inventory/reservations/%d",
		serviceURL(
			"INVENTORY_SERVICE_URL",
			defaultInventoryServiceURL,
		),
		orderID,
	)

	return callSagaService(
		parentContext,
		http.MethodDelete,
		url,
		nil,
	)
}

// reserveDelivery резервирует курьера.
func reserveDelivery(
	parentContext context.Context,
	request DeliveryReservationRequest,
) error {
	url := fmt.Sprintf(
		"%s/delivery/reservations",
		serviceURL(
			"DELIVERY_SERVICE_URL",
			defaultDeliveryServiceURL,
		),
	)

	return callSagaService(
		parentContext,
		http.MethodPost,
		url,
		request,
	)
}

// serviceURL возвращает адрес микросервиса.
func serviceURL(
	envName string,
	defaultURL string,
) string {
	value := strings.TrimSpace(
		getEnv(
			envName,
			defaultURL,
		),
	)

	return strings.TrimRight(
		value,
		"/",
	)
}

// callSagaService выполняет синхронный HTTP-вызов шага Saga.
func callSagaService(
	parentContext context.Context,
	method string,
	url string,
	requestBody any,
) error {
	var body io.Reader

	if requestBody != nil {
		payload, err := json.Marshal(
			requestBody,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to encode request: %w",
				err,
			)
		}

		body = bytes.NewReader(
			payload,
		)
	}

	ctx, cancel := context.WithTimeout(
		parentContext,
		sagaHTTPTimeout,
	)
	defer cancel()

	request, err := http.NewRequestWithContext(
		ctx,
		method,
		url,
		body,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to create request: %w",
			err,
		)
	}

	if requestBody != nil {
		request.Header.Set(
			"Content-Type",
			"application/json",
		)
	}

	response, err := http.DefaultClient.Do(
		request,
	)
	if err != nil {
		return fmt.Errorf(
			"service unavailable: %w",
			err,
		)
	}

	defer response.Body.Close()

	responseBody, readError := io.ReadAll(
		io.LimitReader(
			response.Body,
			64*1024,
		),
	)
	if readError != nil {
		return fmt.Errorf(
			"failed to read service response: %w",
			readError,
		)
	}

	if response.StatusCode < 200 ||
		response.StatusCode >= 300 {
		message := strings.TrimSpace(
			string(responseBody),
		)

		if message == "" {
			message = response.Status
		}

		return fmt.Errorf(
			"HTTP %d: %s",
			response.StatusCode,
			message,
		)
	}

	return nil
}
