FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -trimpath -ldflags="-s -w" -o notification-service .

FROM alpine:3.23

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

COPY --from=builder /app/notification-service /app/notification-service

USER appuser

EXPOSE 8002

ENTRYPOINT ["/app/notification-service"]
