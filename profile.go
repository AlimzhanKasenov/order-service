package main

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
)

// UpdateProfileRequest описывает поля,
// которые пользователь может изменить в своём профиле.
//
// Указатели позволяют отличить отсутствующее поле
// от переданной пустой строки.
type UpdateProfileRequest struct {
	FirstName *string `json:"firstName"`
	LastName  *string `json:"lastName"`
	Email     *string `json:"email"`
	Phone     *string `json:"phone"`
}

// getProfileHandler возвращает профиль пользователя.
//
// До обработчика запрос проходит через
// profileAuthorizationMiddleware.
// Поэтому пользователь может получить только собственный профиль.
func (app *Application) getProfileHandler(
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

	authenticatedUserID, ok :=
		authenticatedUserIDFromContext(r.Context())

	if !ok {
		writeError(
			w,
			http.StatusUnauthorized,
			"authentication is required",
		)
		return
	}

	if authenticatedUserID != userID {
		writeError(
			w,
			http.StatusForbidden,
			"access to another user's profile is forbidden",
		)
		return
	}

	user, err := app.getUserByID(
		r.Context(),
		userID,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		writeError(
			w,
			http.StatusNotFound,
			"user profile not found",
		)
		return
	}

	if err != nil {
		app.logger.Printf(
			"Ошибка получения профиля пользователя ID %d: %v",
			userID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to get user profile",
		)
		return
	}

	emitStructuredLog(
		"INFO",
		"Пользователь получил собственный профиль",
		map[string]any{
			"event":   "profile_read",
			"user_id": userID,
		},
	)

	writeJSON(
		w,
		http.StatusOK,
		user,
	)
}

// updateProfileHandler изменяет профиль пользователя.
//
// Пользователь не может изменить username и password
// через этот endpoint.
//
// Для изменения пароля в будущем потребуется
// отдельный защищённый сценарий.
func (app *Application) updateProfileHandler(
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

	authenticatedUserID, ok :=
		authenticatedUserIDFromContext(r.Context())

	if !ok {
		writeError(
			w,
			http.StatusUnauthorized,
			"authentication is required",
		)
		return
	}

	if authenticatedUserID != userID {
		writeError(
			w,
			http.StatusForbidden,
			"access to another user's profile is forbidden",
		)
		return
	}

	var request UpdateProfileRequest

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

	if request.FirstName == nil &&
		request.LastName == nil &&
		request.Email == nil &&
		request.Phone == nil {
		writeError(
			w,
			http.StatusBadRequest,
			"at least one profile field must be provided",
		)
		return
	}

	user, err := app.getUserByID(
		r.Context(),
		userID,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		writeError(
			w,
			http.StatusNotFound,
			"user profile not found",
		)
		return
	}

	if err != nil {
		app.logger.Printf(
			"Ошибка получения профиля перед обновлением ID %d: %v",
			userID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to get user profile",
		)
		return
	}

	if request.FirstName != nil {
		user.FirstName = strings.TrimSpace(
			*request.FirstName,
		)
	}

	if request.LastName != nil {
		user.LastName = strings.TrimSpace(
			*request.LastName,
		)
	}

	if request.Email != nil {
		user.Email = strings.ToLower(
			strings.TrimSpace(*request.Email),
		)
	}

	if request.Phone != nil {
		user.Phone = strings.TrimSpace(
			*request.Phone,
		)
	}

	if err := validateUser(user); err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			err.Error(),
		)
		return
	}

	ctx, cancel := context.WithTimeout(
		r.Context(),
		databaseRequestTimeout,
	)
	defer cancel()

	query := `
		UPDATE users
		SET
			first_name = $1,
			last_name = $2,
			email = $3,
			phone = $4,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $5
	`

	commandTag, err := app.db.Exec(
		ctx,
		query,
		user.FirstName,
		user.LastName,
		user.Email,
		user.Phone,
		user.ID,
	)

	if err != nil {
		if isUniqueViolation(err) {
			writeError(
				w,
				http.StatusConflict,
				"user with this email already exists",
			)
			return
		}

		app.logger.Printf(
			"Ошибка обновления профиля пользователя ID %d: %v",
			userID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to update user profile",
		)
		return
	}

	if commandTag.RowsAffected() == 0 {
		writeError(
			w,
			http.StatusNotFound,
			"user profile not found",
		)
		return
	}

	emitStructuredLog(
		"INFO",
		"Пользователь изменил собственный профиль",
		map[string]any{
			"event":   "profile_updated",
			"user_id": userID,
		},
	)

	writeJSON(
		w,
		http.StatusOK,
		user,
	)
}

// getUserByID получает профиль пользователя из PostgreSQL.
//
// password_hash намеренно не выбирается и никогда
// не отправляется клиенту.
func (app *Application) getUserByID(
	parentContext context.Context,
	userID int64,
) (User, error) {
	ctx, cancel := context.WithTimeout(
		parentContext,
		databaseRequestTimeout,
	)
	defer cancel()

	query := `
		SELECT
			id,
			username,
			first_name,
			last_name,
			email,
			phone
		FROM users
		WHERE id = $1
	`

	var user User

	err := app.db.QueryRow(
		ctx,
		query,
		userID,
	).Scan(
		&user.ID,
		&user.Username,
		&user.FirstName,
		&user.LastName,
		&user.Email,
		&user.Phone,
	)

	return user, err
}
