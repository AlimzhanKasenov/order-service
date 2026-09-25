package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// reserveInboxEventTx регистрирует входящее Kafka-событие.
//
// true — событие получено впервые.
// false — событие уже было обработано.
//
// Inbox и создание Notification должны быть
// частью одной PostgreSQL-транзакции.
func reserveInboxEventTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID string,
	topic string,
) (bool, error) {
	eventID = strings.TrimSpace(eventID)
	topic = strings.TrimSpace(topic)

	if eventID == "" {
		return false, fmt.Errorf(
			"inbox event id must not be empty",
		)
	}

	if topic == "" {
		return false, fmt.Errorf(
			"inbox topic must not be empty",
		)
	}

	result, err := tx.Exec(
		ctx,
		`
		INSERT INTO inbox_events
		(
			event_id,
			topic
		)
		VALUES ($1, $2)
		ON CONFLICT (event_id) DO NOTHING
		`,
		eventID,
		topic,
	)

	if err != nil {
		return false, fmt.Errorf(
			"failed to insert inbox event: %w",
			err,
		)
	}

	return result.RowsAffected() == 1, nil
}
