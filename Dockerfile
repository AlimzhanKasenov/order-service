FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY main.go ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o order-service .

FROM alpine:3.23

WORKDIR /app

COPY --from=builder /app/order-service ./order-service

EXPOSE 8000

CMD ["./order-service"]