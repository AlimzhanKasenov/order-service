package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// processPaymentNotification атомарно:
//
//  1. сохраняет eventId в Inbox;
//  2. создаёт Notification;
//  3. выполняет COMMIT.
//
// duplicate=true означает, что Kafka-событие
// уже было обработано ранее.
func (app *Application) processPaymentNotification(
	parentContext context.Context,
	event PaymentEvent,
	topic string,
) (
	Notification,
	bool,
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
		return Notification{}, false, fmt.Errorf(
			"failed to begin notification transaction: %w",
			err,
		)
	}

	defer tx.Rollback(ctx)

	firstProcessing, err := reserveInboxEventTx(
		ctx,
		tx,
		event.EventID,
		topic,
	)
	if err != nil {
		return Notification{}, false, err
	}

	if !firstProcessing {
		return Notification{}, true, nil
	}

	request := buildNotificationFromPaymentEvent(
		event,
	)

	var notification Notification

	err = tx.QueryRow(
		ctx,
		`
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
		`,
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

	if err != nil {
		return Notification{}, false, fmt.Errorf(
			"failed to create notification: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return Notification{}, false, fmt.Errorf(
			"failed to commit notification transaction: %w",
			err,
		)
	}

	return notification, false, nil
}
