package main

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// AccessTokenClaims описывает данные, которые хранятся внутри JWT.
//
// Subject содержит ID пользователя.
// Username добавлен для удобства диагностики,
// но авторизация выполняется только по Subject.
type AccessTokenClaims struct {
	Username string `json:"username"`

	jwt.RegisteredClaims
}

// authenticatedUserIDContextKey используется как ключ для хранения
// ID аутентифицированного пользователя в context.Context.
type authenticatedUserIDContextKey struct{}

// profileAuthorizationMiddleware:
//
//  1. Проверяет наличие Bearer JWT.
//  2. Проверяет подпись, issuer и срок действия JWT.
//  3. Получает ID пользователя из JWT subject.
//  4. Сравнивает его с userId в URL.
//  5. Возвращает 403, если пользователь обращается к чужому профилю.
func (app *Application) profileAuthorizationMiddleware(
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		tokenValue, err := bearerTokenFromRequest(r)
		if err != nil {
			writeError(
				w,
				http.StatusUnauthorized,
				"authentication is required",
			)
			return
		}

		claims, err := app.parseAccessToken(tokenValue)
		if err != nil {
			app.logger.Printf(
				"Отклонён недействительный JWT: %v",
				err,
			)

			writeError(
				w,
				http.StatusUnauthorized,
				"invalid or expired access token",
			)
			return
		}

		authenticatedUserID, err :=
			strconv.ParseInt(claims.Subject, 10, 64)

		if err != nil || authenticatedUserID <= 0 {
			writeError(
				w,
				http.StatusUnauthorized,
				"invalid access token subject",
			)
			return
		}

		requestedUserID, err := parseUserID(r)
		if err != nil {
			writeError(
				w,
				http.StatusBadRequest,
				err.Error(),
			)
			return
		}

		if requestedUserID != authenticatedUserID {
			app.logger.Printf(
				"Запрещён доступ к чужому профилю: authenticated_user_id=%d requested_user_id=%d",
				authenticatedUserID,
				requestedUserID,
			)

			writeError(
				w,
				http.StatusForbidden,
				"access to another user's profile is forbidden",
			)
			return
		}

		ctx := context.WithValue(
			r.Context(),
			authenticatedUserIDContextKey{},
			authenticatedUserID,
		)

		next.ServeHTTP(
			w,
			r.WithContext(ctx),
		)
	})
}

// bearerTokenFromRequest извлекает JWT из заголовка:
//
// Authorization: Bearer <token>
func bearerTokenFromRequest(
	r *http.Request,
) (string, error) {
	authorizationHeader :=
		strings.TrimSpace(r.Header.Get("Authorization"))

	if authorizationHeader == "" {
		return "", fmt.Errorf(
			"authorization header is missing",
		)
	}

	parts := strings.Fields(authorizationHeader)

	if len(parts) != 2 ||
		!strings.EqualFold(parts[0], "Bearer") ||
		strings.TrimSpace(parts[1]) == "" {
		return "", fmt.Errorf(
			"authorization header must use Bearer scheme",
		)
	}

	return parts[1], nil
}

// parseAccessToken проверяет JWT и возвращает его claims.
func (app *Application) parseAccessToken(
	tokenValue string,
) (*AccessTokenClaims, error) {
	claims := &AccessTokenClaims{}

	token, err := jwt.ParseWithClaims(
		tokenValue,
		claims,
		func(token *jwt.Token) (any, error) {
			if token.Method.Alg() !=
				jwt.SigningMethodHS256.Alg() {
				return nil, fmt.Errorf(
					"unexpected JWT signing algorithm: %s",
					token.Method.Alg(),
				)
			}

			return app.jwtSecret, nil
		},
		jwt.WithValidMethods(
			[]string{
				jwt.SigningMethodHS256.Alg(),
			},
		),
		jwt.WithIssuer(app.jwtIssuer),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithLeeway(5*time.Second),
	)

	if err != nil {
		return nil, fmt.Errorf(
			"failed to validate JWT: %w",
			err,
		)
	}

	if !token.Valid {
		return nil, fmt.Errorf(
			"JWT is not valid",
		)
	}

	if strings.TrimSpace(claims.Subject) == "" {
		return nil, fmt.Errorf(
			"JWT subject is empty",
		)
	}

	return claims, nil
}

// authenticatedUserIDFromContext возвращает ID пользователя,
// сохранённый middleware после успешной проверки JWT.
func authenticatedUserIDFromContext(
	ctx context.Context,
) (int64, bool) {
	userID, ok := ctx.Value(
		authenticatedUserIDContextKey{},
	).(int64)

	return userID, ok
}
