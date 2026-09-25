package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// processUserCreatedEvent обрабатывает user.created
// через Inbox Pattern.
//
// Inbox и создание счёта выполняются в одной транзакции.
// duplicate=true означает, что eventId уже был обработан.
func (app *Application) processUserCreatedEvent(
	parentContext context.Context,
	event UserCreatedEvent,
) (
	Account,
	bool,
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
		return Account{}, false, false, fmt.Errorf(
			"failed to begin user.created transaction: %w",
			err,
		)
	}

	defer tx.Rollback(ctx)

	firstProcessing, err := reserveInboxEventTx(
		ctx,
		tx,
		event.EventID,
		userCreatedTopic,
	)
	if err != nil {
		return Account{}, false, false, err
	}

	if !firstProcessing {
		return Account{}, false, true, nil
	}

	var account Account

	err = tx.QueryRow(
		ctx,
		`
		INSERT INTO accounts
		(
			user_id,
			balance
		)
		VALUES ($1, 0)
		ON CONFLICT (user_id) DO NOTHING
		RETURNING
			id,
			user_id,
			balance,
			created_at,
			updated_at
		`,
		event.UserID,
	).Scan(
		&account.ID,
		&account.UserID,
		&account.Balance,
		&account.CreatedAt,
		&account.UpdatedAt,
	)

	created := true

	if err == pgx.ErrNoRows {
		created = false

		err = tx.QueryRow(
			ctx,
			`
			SELECT
				id,
				user_id,
				balance,
				created_at,
				updated_at
			FROM accounts
			WHERE user_id = $1
			`,
			event.UserID,
		).Scan(
			&account.ID,
			&account.UserID,
			&account.Balance,
			&account.CreatedAt,
			&account.UpdatedAt,
		)
	}

	if err != nil {
		return Account{}, false, false, fmt.Errorf(
			"failed to create or read billing account: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return Account{}, false, false, fmt.Errorf(
			"failed to commit user.created transaction: %w",
			err,
		)
	}

	return account, created, false, nil
}

// processOrderCreatedEvent выполняет оплату заказа.
//
// В одной PostgreSQL-транзакции:
//
//  1. регистрирует eventId в Inbox;
//  2. блокирует счёт через SELECT ... FOR UPDATE;
//  3. при необходимости изменяет баланс;
//  4. создаёт payment.succeeded/payment.failed в Outbox;
//  5. выполняет COMMIT.
//
// duplicate=true означает, что событие уже было обработано.
func (app *Application) processOrderCreatedEvent(
	parentContext context.Context,
	event OrderCreatedEvent,
) (
	Account,
	bool,
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
		return Account{}, false, false, fmt.Errorf(
			"failed to begin order.created transaction: %w",
			err,
		)
	}

	defer tx.Rollback(ctx)

	firstProcessing, err := reserveInboxEventTx(
		ctx,
		tx,
		event.EventID,
		orderCreatedTopic,
	)
	if err != nil {
		return Account{}, false, false, err
	}

	if !firstProcessing {
		return Account{}, false, true, nil
	}

	account, err := lockAccountForUpdateTx(
		ctx,
		tx,
		event.UserID,
	)
	if err != nil {
		return Account{}, false, false, err
	}

	sufficientFunds := account.Balance >= event.Price

	if sufficientFunds {
		account.Balance -= event.Price

		err = updateAccountBalanceTx(
			ctx,
			tx,
			account.ID,
			account.Balance,
		)
		if err != nil {
			return Account{}, false, false, err
		}

		account.UpdatedAt = time.Now().UTC()
	}

	topic := paymentFailedTopic
	status := "FAILED"

	if sufficientFunds {
		topic = paymentSucceededTopic
		status = "SUCCESS"
	}

	paymentEvent := PaymentEvent{
		OrderID:     event.OrderID,
		UserID:      event.UserID,
		Price:       event.Price,
		Email:       event.Email,
		Balance:     account.Balance,
		Status:      status,
		ProcessedAt: time.Now().UTC(),
	}

	_, err = insertOutboxEventTx(
		ctx,
		tx,
		topic,
		strconv.FormatInt(
			event.OrderID,
			10,
		),
		paymentEvent,
	)
	if err != nil {
		return Account{}, false, false, fmt.Errorf(
			"failed to create payment outbox event: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return Account{}, false, false, fmt.Errorf(
			"failed to commit order.created transaction: %w",
			err,
		)
	}

	return account, sufficientFunds, false, nil
}

// lockAccountForUpdateTx читает счёт и устанавливает
// PostgreSQL row-level lock до завершения транзакции.
//
// Это не позволяет двум параллельным операциям одновременно
// прочитать старое значение balance и затереть изменения друг друга.
func lockAccountForUpdateTx(
	ctx context.Context,
	tx pgx.Tx,
	userID int64,
) (
	Account,
	error,
) {
	var account Account

	err := tx.QueryRow(
		ctx,
		`
		SELECT
			id,
			user_id,
			balance,
			created_at,
			updated_at
		FROM accounts
		WHERE user_id = $1
		FOR UPDATE
		`,
		userID,
	).Scan(
		&account.ID,
		&account.UserID,
		&account.Balance,
		&account.CreatedAt,
		&account.UpdatedAt,
	)

	if err != nil {
		return Account{}, err
	}

	return account, nil
}

// updateAccountBalanceTx изменяет баланс уже заблокированного счёта.
func updateAccountBalanceTx(
	ctx context.Context,
	tx pgx.Tx,
	accountID int64,
	balance int64,
) error {
	_, err := tx.Exec(
		ctx,
		`
		UPDATE accounts
		SET
			balance = $2,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		`,
		accountID,
		balance,
	)

	if err != nil {
		return fmt.Errorf(
			"failed to update account balance: %w",
			err,
		)
	}

	return nil
}
