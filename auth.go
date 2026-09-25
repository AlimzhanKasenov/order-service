package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
)

const (
	bcryptCost        = 12
	minPasswordLength = 8
	maxPasswordLength = 72
)

// RegisterRequest описывает запрос регистрации пользователя.
type RegisterRequest struct {
	Username  string `json:"username"`
	Password  string `json:"password"`
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
	Email     string `json:"email"`
	Phone     string `json:"phone"`
}

// LoginRequest описывает запрос входа пользователя.
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// LoginResponse возвращается после успешного входа.
type LoginResponse struct {
	TokenType   string `json:"tokenType"`
	AccessToken string `json:"accessToken"`
	ExpiresIn   int64  `json:"expiresIn"`
	UserID      int64  `json:"userId"`
}

// registerHandler регистрирует нового пользователя.
//
// После создания пользователя публикуется Kafka-событие
// user.created. Billing Service получает это событие
// и автоматически создаёт счёт пользователя.
func (app *Application) registerHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request RegisterRequest

	if err := decodeJSON(w, r, &request); err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			err.Error(),
		)
		return
	}

	user := User{
		Username: strings.ToLower(
			strings.TrimSpace(request.Username),
		),
		FirstName: strings.TrimSpace(
			request.FirstName,
		),
		LastName: strings.TrimSpace(
			request.LastName,
		),
		Email: strings.ToLower(
			strings.TrimSpace(request.Email),
		),
		Phone: strings.TrimSpace(
			request.Phone,
		),
	}

	if err := validateUser(user); err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			err.Error(),
		)
		return
	}

	if err := validatePassword(request.Password); err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			err.Error(),
		)
		return
	}

	passwordHash, err := bcrypt.GenerateFromPassword(
		[]byte(request.Password),
		bcryptCost,
	)
	if err != nil {
		app.logger.Printf(
			"Ошибка создания BCrypt-хеша: %v",
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to register user",
		)
		return
	}

	user, err = app.createUserWithOutbox(
		r.Context(),
		user,
		string(passwordHash),
	)

	if err != nil {
		if isUniqueViolation(err) {
			writeError(
				w,
				http.StatusConflict,
				"user with this username or email already exists",
			)
			return
		}

		app.logger.Printf(
			"Ошибка регистрации пользователя: %v",
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to register user",
		)
		return
	}

	emitStructuredLog(
		"INFO",
		"Пользователь зарегистрирован",
		map[string]any{
			"event":    "user_registered",
			"user_id":  user.ID,
			"username": user.Username,
		},
	)

	emitStructuredLog(
		"INFO",
		"Событие user.created сохранено в Transactional Outbox",
		map[string]any{
			"event":   "user_created_outbox",
			"user_id": user.ID,
			"topic":   userCreatedTopic,
		},
	)

	writeJSON(
		w,
		http.StatusCreated,
		user,
	)
}

// loginHandler проверяет логин и пароль и выдаёт JWT.
func (app *Application) loginHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request LoginRequest

	if err := decodeJSON(w, r, &request); err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			err.Error(),
		)
		return
	}

	username := strings.ToLower(
		strings.TrimSpace(request.Username),
	)

	if username == "" || request.Password == "" {
		writeError(
			w,
			http.StatusBadRequest,
			"username and password are required",
		)
		return
	}

	ctx, cancel := contextWithDatabaseTimeout(
		r,
	)
	defer cancel()

	var user User
	var passwordHash sql.NullString

	query := `
		SELECT
			id,
			username,
			password_hash
		FROM users
		WHERE username = $1
	`

	err := app.db.QueryRow(
		ctx,
		query,
		username,
	).Scan(
		&user.ID,
		&user.Username,
		&passwordHash,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		writeInvalidCredentials(w)
		return
	}

	if err != nil {
		app.logger.Printf(
			"Ошибка получения пользователя при входе: %v",
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to log in",
		)
		return
	}

	if !passwordHash.Valid ||
		strings.TrimSpace(passwordHash.String) == "" {
		writeInvalidCredentials(w)
		return
	}

	err = bcrypt.CompareHashAndPassword(
		[]byte(passwordHash.String),
		[]byte(request.Password),
	)
	if err != nil {
		writeInvalidCredentials(w)
		return
	}

	accessToken, expiresIn, err :=
		app.createAccessToken(user)

	if err != nil {
		app.logger.Printf(
			"Ошибка создания JWT для пользователя ID %d: %v",
			user.ID,
			err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to create access token",
		)
		return
	}

	emitStructuredLog(
		"INFO",
		"Пользователь успешно вошёл",
		map[string]any{
			"event":    "user_logged_in",
			"user_id":  user.ID,
			"username": user.Username,
		},
	)

	writeJSON(
		w,
		http.StatusOK,
		LoginResponse{
			TokenType:   "Bearer",
			AccessToken: accessToken,
			ExpiresIn:   expiresIn,
			UserID:      user.ID,
		},
	)
}

// createAccessToken создаёт подписанный JWT.
func (app *Application) createAccessToken(
	user User,
) (string, int64, error) {
	now := time.Now().UTC()
	expiresAt := now.Add(app.jwtTTL)

	tokenID, err := newTokenID()
	if err != nil {
		return "", 0, fmt.Errorf(
			"failed to create token ID: %w",
			err,
		)
	}

	claims := AccessTokenClaims{
		Username: user.Username,

		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    app.jwtIssuer,
			Subject:   strconv.FormatInt(user.ID, 10),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			NotBefore: jwt.NewNumericDate(now),
			IssuedAt:  jwt.NewNumericDate(now),
			ID:        tokenID,
		},
	}

	token := jwt.NewWithClaims(
		jwt.SigningMethodHS256,
		claims,
	)

	tokenValue, err := token.SignedString(
		app.jwtSecret,
	)
	if err != nil {
		return "", 0, fmt.Errorf(
			"failed to sign JWT: %w",
			err,
		)
	}

	return tokenValue, int64(app.jwtTTL.Seconds()), nil
}

// validatePassword проверяет ограничения BCrypt.
func validatePassword(
	password string,
) error {
	passwordLength := len(
		[]byte(password),
	)

	if passwordLength < minPasswordLength {
		return fmt.Errorf(
			"password must contain at least %d characters",
			minPasswordLength,
		)
	}

	if passwordLength > maxPasswordLength {
		return fmt.Errorf(
			"password must not be longer than %d bytes",
			maxPasswordLength,
		)
	}

	return nil
}

// newTokenID создаёт случайный идентификатор JWT.
func newTokenID() (string, error) {
	randomBytes := make([]byte, 16)

	if _, err := rand.Read(randomBytes); err != nil {
		return "", err
	}

	return hex.EncodeToString(randomBytes), nil
}

// writeInvalidCredentials возвращает одинаковую ошибку
// для неизвестного логина и неправильного пароля.
func writeInvalidCredentials(
	w http.ResponseWriter,
) {
	writeError(
		w,
		http.StatusUnauthorized,
		"invalid username or password",
	)
}

// contextWithDatabaseTimeout создаёт контекст запроса к PostgreSQL.
func contextWithDatabaseTimeout(
	r *http.Request,
) (
	context.Context,
	context.CancelFunc,
) {
	return context.WithTimeout(
		r.Context(),
		databaseRequestTimeout,
	)
}
