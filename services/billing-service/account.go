package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

// Account описывает счёт пользователя.
//
// Суммы хранятся как целое число.
// Для нашего учебного задания считаем,
// что это денежные единицы без дробной части.
type Account struct {
	ID        int64     `json:"id"`
	UserID    int64     `json:"userId"`
	Balance   int64     `json:"balance"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// CreateAccountRequest создаёт счёт пользователя.
type CreateAccountRequest struct {
	UserID int64 `json:"userId"`
}

// MoneyRequest используется для пополнения
// и списания денежных средств.
type MoneyRequest struct {
	Amount int64 `json:"amount"`
}

// createAccountHandler создаёт счёт.
//
// Повторное создание счёта одного пользователя запрещено.
func (app *Application) createAccountHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request CreateAccountRequest

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

	if request.UserID <= 0 {
		writeError(
			w,
			http.StatusBadRequest,
			"userId must be a positive integer",
		)
		return
	}

	account, err := app.createAccount(
		r.Context(),
		request.UserID,
	)

	if err != nil {
		if isUniqueViolation(err) {
			writeError(
				w,
				http.StatusConflict,
				"account for this user already exists",
			)
			return
		}

		app.logger.Printf(
			"Ошибка создания счёта пользователя %d: %v",
			request.UserID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to create account",
		)
		return
	}

	app.logger.Printf(
		"Создан счёт пользователя user_id=%d account_id=%d",
		account.UserID,
		account.ID,
	)

	writeJSON(
		w,
		http.StatusCreated,
		account,
	)
}

// getAccountHandler возвращает счёт пользователя.
func (app *Application) getAccountHandler(
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

	account, err := app.getAccount(
		r.Context(),
		userID,
	)

	if errors.Is(
		err,
		pgx.ErrNoRows,
	) {
		writeError(
			w,
			http.StatusNotFound,
			"account not found",
		)
		return
	}

	if err != nil {
		app.logger.Printf(
			"Ошибка получения счёта пользователя %d: %v",
			userID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to get account",
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		account,
	)
}

// depositHandler пополняет счёт пользователя.
func (app *Application) depositHandler(
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

	var request MoneyRequest

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

	if request.Amount <= 0 {
		writeError(
			w,
			http.StatusBadRequest,
			"amount must be greater than zero",
		)
		return
	}

	account, err := app.deposit(
		r.Context(),
		userID,
		request.Amount,
	)

	if errors.Is(
		err,
		pgx.ErrNoRows,
	) {
		writeError(
			w,
			http.StatusNotFound,
			"account not found",
		)
		return
	}

	if err != nil {
		app.logger.Printf(
			"Ошибка пополнения счёта пользователя %d: %v",
			userID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to deposit money",
		)
		return
	}

	app.logger.Printf(
		"Счёт пользователя %d пополнен на %d",
		userID,
		request.Amount,
	)

	writeJSON(
		w,
		http.StatusOK,
		account,
	)
}

// withdrawHandler списывает деньги со счёта.
//
// Если денег недостаточно,
// баланс вообще не изменяется.
func (app *Application) withdrawHandler(
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

	var request MoneyRequest

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

	if request.Amount <= 0 {
		writeError(
			w,
			http.StatusBadRequest,
			"amount must be greater than zero",
		)
		return
	}

	account, sufficientFunds, err := app.withdraw(
		r.Context(),
		userID,
		request.Amount,
	)

	if errors.Is(
		err,
		pgx.ErrNoRows,
	) {
		writeError(
			w,
			http.StatusNotFound,
			"account not found",
		)
		return
	}

	if err != nil {
		app.logger.Printf(
			"Ошибка списания со счёта пользователя %d: %v",
			userID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to withdraw money",
		)
		return
	}

	if !sufficientFunds {
		writeError(
			w,
			http.StatusConflict,
			"insufficient funds",
		)
		return
	}

	app.logger.Printf(
		"Со счёта пользователя %d списано %d",
		userID,
		request.Amount,
	)

	writeJSON(
		w,
		http.StatusOK,
		account,
	)
}

// createAccount создаёт счёт с нулевым балансом.
//
// Эту функцию позже будет использовать Kafka consumer
// события user.created.
func (app *Application) createAccount(
	parentContext context.Context,
	userID int64,
) (Account, error) {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	var account Account

	query := `
		INSERT INTO accounts
		(
			user_id,
			balance
		)
		VALUES ($1, 0)
		RETURNING
			id,
			user_id,
			balance,
			created_at,
			updated_at
	`

	err := app.db.QueryRow(
		ctx,
		query,
		userID,
	).Scan(
		&account.ID,
		&account.UserID,
		&account.Balance,
		&account.CreatedAt,
		&account.UpdatedAt,
	)

	return account, err
}

// getAccount получает счёт пользователя.
func (app *Application) getAccount(
	parentContext context.Context,
	userID int64,
) (Account, error) {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	var account Account

	query := `
		SELECT
			id,
			user_id,
			balance,
			created_at,
			updated_at
		FROM accounts
		WHERE user_id = $1
	`

	err := app.db.QueryRow(
		ctx,
		query,
		userID,
	).Scan(
		&account.ID,
		&account.UserID,
		&account.Balance,
		&account.CreatedAt,
		&account.UpdatedAt,
	)

	return account, err
}

// deposit атомарно пополняет баланс.
func (app *Application) deposit(
	parentContext context.Context,
	userID int64,
	amount int64,
) (
	Account,
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
		return Account{}, err
	}

	defer tx.Rollback(ctx)

	account, err := lockAccountForUpdateTx(
		ctx,
		tx,
		userID,
	)
	if err != nil {
		return Account{}, err
	}

	account.Balance += amount

	if err := updateAccountBalanceTx(
		ctx,
		tx,
		account.ID,
		account.Balance,
	); err != nil {
		return Account{}, err
	}

	if err := tx.QueryRow(
		ctx,
		`
		SELECT
			id,
			user_id,
			balance,
			created_at,
			updated_at
		FROM accounts
		WHERE id = $1
		`,
		account.ID,
	).Scan(
		&account.ID,
		&account.UserID,
		&account.Balance,
		&account.CreatedAt,
		&account.UpdatedAt,
	); err != nil {
		return Account{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Account{}, err
	}

	return account, nil
}

// withdraw атомарно списывает деньги.
//
// Условие balance >= amount находится прямо в UPDATE,
// поэтому баланс не сможет уйти в минус.
func (app *Application) withdraw(
	parentContext context.Context,
	userID int64,
	amount int64,
) (
	Account,
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
		return Account{}, false, err
	}

	defer tx.Rollback(ctx)

	account, err := lockAccountForUpdateTx(
		ctx,
		tx,
		userID,
	)
	if err != nil {
		return Account{}, false, err
	}

	if account.Balance < amount {
		if err := tx.Commit(ctx); err != nil {
			return Account{}, false, err
		}

		return account, false, nil
	}

	account.Balance -= amount

	if err := updateAccountBalanceTx(
		ctx,
		tx,
		account.ID,
		account.Balance,
	); err != nil {
		return Account{}, false, err
	}

	if err := tx.QueryRow(
		ctx,
		`
		SELECT
			id,
			user_id,
			balance,
			created_at,
			updated_at
		FROM accounts
		WHERE id = $1
		`,
		account.ID,
	).Scan(
		&account.ID,
		&account.UserID,
		&account.Balance,
		&account.CreatedAt,
		&account.UpdatedAt,
	); err != nil {
		return Account{}, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Account{}, false, err
	}

	return account, true, nil
}
