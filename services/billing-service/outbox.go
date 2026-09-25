package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"
)

const (
	outboxPollInterval = 500 * time.Millisecond
	outboxWriteTimeout = 5 * time.Second
)

// OutboxEvent представляет событие,
// ожидающее отправки в Kafka.
type OutboxEvent struct {
	ID       int64
	EventID  string
	Topic    string
	EventKey string
	Payload  []byte
}

// newEventID генерирует UUID v4 без внешней библиотеки.
func newEventID() (string, error) {
	value := make([]byte, 16)

	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf(
			"failed to generate event id: %w",
			err,
		)
	}

	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80

	return fmt.Sprintf(
		"%x-%x-%x-%x-%x",
		value[0:4],
		value[4:6],
		value[6:8],
		value[8:10],
		value[10:16],
	), nil
}

// insertOutboxEventTx сохраняет событие
// в той же транзакции, что и бизнес-операция.
func insertOutboxEventTx(
	ctx context.Context,
	tx pgx.Tx,
	topic string,
	eventKey string,
	event any,
) (string, error) {
	topic = strings.TrimSpace(topic)
	eventKey = strings.TrimSpace(eventKey)

	if topic == "" {
		return "", fmt.Errorf(
			"outbox topic must not be empty",
		)
	}

	if eventKey == "" {
		return "", fmt.Errorf(
			"outbox event key must not be empty",
		)
	}

	eventID, err := newEventID()
	if err != nil {
		return "", err
	}

	encodedEvent, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf(
			"failed to encode outbox event: %w",
			err,
		)
	}

	var payload map[string]any

	if err := json.Unmarshal(
		encodedEvent,
		&payload,
	); err != nil {
		return "", fmt.Errorf(
			"outbox event must be a JSON object: %w",
			err,
		)
	}

	payload["eventId"] = eventID

	encodedPayload, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf(
			"failed to encode outbox payload: %w",
			err,
		)
	}

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO outbox_events
		(
			event_id,
			topic,
			event_key,
			payload
		)
		VALUES ($1, $2, $3, $4::jsonb)
		`,
		eventID,
		topic,
		eventKey,
		string(encodedPayload),
	)

	if err != nil {
		return "", fmt.Errorf(
			"failed to insert outbox event: %w",
			err,
		)
	}

	return eventID, nil
}

// reserveInboxEventTx регистрирует Kafka-событие.
//
// false означает, что событие с таким event_id
// уже обрабатывалось ранее.
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

// runOutboxPublisher постоянно доставляет
// PENDING-события Billing Service в Kafka.
func (app *Application) runOutboxPublisher(
	ctx context.Context,
) {
	app.logger.Println(
		"Transactional Outbox publisher запущен",
	)

	for {
		if ctx.Err() != nil {
			return
		}

		processed, err := app.publishOneOutboxEvent(
			ctx,
		)

		if err != nil {
			app.logger.Printf(
				"Ошибка Outbox publisher: %v",
				err,
			)

			if !waitOutboxInterval(ctx) {
				return
			}

			continue
		}

		if processed {
			continue
		}

		if !waitOutboxInterval(ctx) {
			return
		}
	}
}

// publishOneOutboxEvent публикует одно событие.
//
// FOR UPDATE SKIP LOCKED предотвращает параллельную
// обработку одной строки несколькими экземплярами сервиса.
func (app *Application) publishOneOutboxEvent(
	parentContext context.Context,
) (bool, error) {
	ctx, cancel := context.WithTimeout(
		parentContext,
		10*time.Second,
	)
	defer cancel()

	tx, err := app.db.BeginTx(
		ctx,
		pgx.TxOptions{
			IsoLevel: pgx.ReadCommitted,
		},
	)
	if err != nil {
		return false, fmt.Errorf(
			"failed to begin outbox transaction: %w",
			err,
		)
	}

	defer tx.Rollback(ctx)

	var event OutboxEvent

	err = tx.QueryRow(
		ctx,
		`
		SELECT
			id,
			event_id,
			topic,
			event_key,
			payload
		FROM outbox_events
		WHERE status = 'PENDING'
		ORDER BY id
		FOR UPDATE SKIP LOCKED
		LIMIT 1
		`,
	).Scan(
		&event.ID,
		&event.EventID,
		&event.Topic,
		&event.EventKey,
		&event.Payload,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}

		return false, fmt.Errorf(
			"failed to read outbox event: %w",
			err,
		)
	}

	writer := &kafka.Writer{
		Addr: kafka.TCP(
			getKafkaBroker(),
		),
		Topic:    event.Topic,
		Balancer: &kafka.LeastBytes{},
	}

	writeContext, cancelWrite := context.WithTimeout(
		ctx,
		outboxWriteTimeout,
	)

	writeErr := writer.WriteMessages(
		writeContext,
		kafka.Message{
			Key:   []byte(event.EventKey),
			Value: event.Payload,
			Time:  time.Now().UTC(),
		},
	)

	cancelWrite()

	closeErr := writer.Close()

	if writeErr != nil {
		_, updateErr := tx.Exec(
			ctx,
			`
			UPDATE outbox_events
			SET
				attempts = attempts + 1,
				last_error = $2
			WHERE id = $1
			`,
			event.ID,
			writeErr.Error(),
		)

		if updateErr != nil {
			return true, fmt.Errorf(
				"Kafka publish failed: %v; failed to update outbox error: %w",
				writeErr,
				updateErr,
			)
		}

		if commitErr := tx.Commit(ctx); commitErr != nil {
			return true, fmt.Errorf(
				"Kafka publish failed: %v; failed to commit outbox error: %w",
				writeErr,
				commitErr,
			)
		}

		return true, fmt.Errorf(
			"failed to publish outbox event %s to %s: %w",
			event.EventID,
			event.Topic,
			writeErr,
		)
	}

	if closeErr != nil {
		app.logger.Printf(
			"Предупреждение закрытия Kafka writer: %v",
			closeErr,
		)
	}

	_, err = tx.Exec(
		ctx,
		`
		UPDATE outbox_events
		SET
			status = 'PUBLISHED',
			published_at = CURRENT_TIMESTAMP,
			last_error = NULL
		WHERE id = $1
		`,
		event.ID,
	)

	if err != nil {
		return true, fmt.Errorf(
			"failed to mark outbox event as published: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return true, fmt.Errorf(
			"failed to commit published outbox event: %w",
			err,
		)
	}

	app.logger.Printf(
		"Outbox event опубликован: event_id=%s topic=%s key=%s",
		event.EventID,
		event.Topic,
		event.EventKey,
	)

	return true, nil
}

func waitOutboxInterval(
	ctx context.Context,
) bool {
	timer := time.NewTimer(
		outboxPollInterval,
	)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false

	case <-timer.C:
		return true
	}
}
