FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build \
    -trimpath \
    -ldflags="-s -w" \
    -o inventory-service \
    .

FROM alpine:3.23

RUN addgroup -S appgroup \
    && adduser -S appuser -G appgroup

WORKDIR /app

COPY --from=builder \
    /app/inventory-service \
    /app/inventory-service

USER appuser

EXPOSE 8003

ENTRYPOINT ["/app/inventory-service"]