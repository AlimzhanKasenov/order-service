FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build \
    -trimpath \
    -ldflags="-s -w" \
    -o delivery-service \
    .

FROM alpine:3.23

RUN addgroup -S appgroup \
    && adduser -S appuser -G appgroup

WORKDIR /app

COPY --from=builder \
    /app/delivery-service \
    /app/delivery-service

USER appuser

EXPOSE 8004

ENTRYPOINT ["/app/delivery-service"]
