package main

import (
	"context"
	"net/http"
	"strings"
	"time"
)

// Notification описывает сохранённое уведомление.
type Notification struct {
	ID        int64     `json:"id"`
	UserID    int64     `json:"userId"`
	OrderID   int64     `json:"orderId"`
	Email     string    `json:"email"`
	Subject   string    `json:"subject"`
	Message   string    `json:"message"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
}

// CreateNotificationRequest описывает запрос
// на создание уведомления.
type CreateNotificationRequest struct {
	UserID  int64  `json:"userId"`
	OrderID int64  `json:"orderId"`
	Email   string `json:"email"`
	Subject string `json:"subject"`
	Message string `json:"message"`
	Status  string `json:"status"`
}

// createNotificationHandler сохраняет уведомление.
func (app *Application) createNotificationHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request CreateNotificationRequest

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

	request.Email = strings.TrimSpace(
		request.Email,
	)

	request.Subject = strings.TrimSpace(
		request.Subject,
	)

	request.Message = strings.TrimSpace(
		request.Message,
	)

	request.Status = strings.TrimSpace(
		request.Status,
	)

	if request.UserID <= 0 {
		writeError(
			w,
			http.StatusBadRequest,
			"userId must be a positive integer",
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

	if request.Email == "" {
		writeError(
			w,
			http.StatusBadRequest,
			"email is required",
		)
		return
	}

	if request.Subject == "" {
		writeError(
			w,
			http.StatusBadRequest,
			"subject is required",
		)
		return
	}

	if request.Message == "" {
		writeError(
			w,
			http.StatusBadRequest,
			"message is required",
		)
		return
	}

	if request.Status != "SUCCESS" &&
		request.Status != "FAILED" {
		writeError(
			w,
			http.StatusBadRequest,
			"status must be SUCCESS or FAILED",
		)
		return
	}

	notification, err := app.createNotification(
		r.Context(),
		request,
	)

	if err != nil {
		app.logger.Printf(
			"Ошибка сохранения уведомления: %v",
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to create notification",
		)
		return
	}

	app.logger.Printf(
		"Уведомление сохранено user_id=%d order_id=%d status=%s",
		notification.UserID,
		notification.OrderID,
		notification.Status,
	)

	writeJSON(
		w,
		http.StatusCreated,
		notification,
	)
}

// getNotificationsHandler возвращает
// список уведомлений пользователя.
func (app *Application) getNotificationsHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	userID, err := parseUserID(r)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			err.Error(),
		)
		return
	}

	notifications, err := app.getNotifications(
		r.Context(),
		userID,
	)

	if err != nil {
		app.logger.Printf(
			"Ошибка получения уведомлений пользователя %d: %v",
			userID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to get notifications",
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		notifications,
	)
}

// createNotification сохраняет уведомление в БД.
func (app *Application) createNotification(
	parentContext context.Context,
	request CreateNotificationRequest,
) (
	Notification,
	error,
) {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	var notification Notification

	query := `
		INSERT INTO notifications
		(
			user_id,
			order_id,
			email,
			subject,
			message,
			status
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING
			id,
			user_id,
			order_id,
			email,
			subject,
			message,
			status,
			created_at
	`

	err := app.db.QueryRow(
		ctx,
		query,
		request.UserID,
		request.OrderID,
		request.Email,
		request.Subject,
		request.Message,
		request.Status,
	).Scan(
		&notification.ID,
		&notification.UserID,
		&notification.OrderID,
		&notification.Email,
		&notification.Subject,
		&notification.Message,
		&notification.Status,
		&notification.CreatedAt,
	)

	return notification, err
}

// getNotifications получает все уведомления пользователя.
func (app *Application) getNotifications(
	parentContext context.Context,
	userID int64,
) (
	[]Notification,
	error,
) {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	query := `
		SELECT
			id,
			user_id,
			order_id,
			email,
			subject,
			message,
			status,
			created_at
		FROM notifications
		WHERE user_id = $1
		ORDER BY id ASC
	`

	rows, err := app.db.Query(
		ctx,
		query,
		userID,
	)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	notifications := make(
		[]Notification,
		0,
	)

	for rows.Next() {
		var notification Notification

		if err := rows.Scan(
			&notification.ID,
			&notification.UserID,
			&notification.OrderID,
			&notification.Email,
			&notification.Subject,
			&notification.Message,
			&notification.Status,
			&notification.CreatedAt,
		); err != nil {
			return nil, err
		}

		notifications = append(
			notifications,
			notification,
		)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return notifications, nil
}
