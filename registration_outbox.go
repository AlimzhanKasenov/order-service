package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// createUserWithOutbox создаёт пользователя и событие user.created
// в одной PostgreSQL-транзакции.
//
// Если COMMIT не выполнится, не сохранится ни пользователь,
// ни запись Outbox.
func (app *Application) createUserWithOutbox(
	parentContext context.Context,
	user User,
	passwordHash string,
) (User, error) {
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
		return User{}, fmt.Errorf(
			"failed to begin registration transaction: %w",
			err,
		)
	}

	defer tx.Rollback(ctx)

	err = tx.QueryRow(
		ctx,
		`
		INSERT INTO users
		(
			username,
			password_hash,
			first_name,
			last_name,
			email,
			phone
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id
		`,
		user.Username,
		passwordHash,
		user.FirstName,
		user.LastName,
		user.Email,
		user.Phone,
	).Scan(
		&user.ID,
	)

	if err != nil {
		return User{}, err
	}

	event := UserCreatedEvent{
		UserID:    user.ID,
		Username:  user.Username,
		Email:     user.Email,
		CreatedAt: time.Now().UTC(),
	}

	_, err = insertOutboxEventTx(
		ctx,
		tx,
		userCreatedTopic,
		strconv.FormatInt(
			user.ID,
			10,
		),
		event,
	)

	if err != nil {
		return User{}, fmt.Errorf(
			"failed to create user.created outbox event: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return User{}, fmt.Errorf(
			"failed to commit registration transaction: %w",
			err,
		)
	}

	return user, nil
}
