#!/usr/bin/env bash

set -u

BASE_URL="${BASE_URL:-http://127.0.0.1:18080}"
HOST_HEADER="${HOST_HEADER:-arch.homework}"
DURATION_SECONDS="${DURATION_SECONDS:-600}"

END_TIME=$((SECONDS + DURATION_SECONDS))
CYCLE=0

echo "Стресс-тест запущен"
echo "URL: ${BASE_URL}"
echo "Host: ${HOST_HEADER}"
echo "Продолжительность: ${DURATION_SECONDS} секунд"

while (( SECONDS < END_TIME )); do
    CYCLE=$((CYCLE + 1))

    # Быстрые запросы.
    for _ in $(seq 1 40); do
        curl \
            --max-time 5 \
            -sS \
            -H "Host: ${HOST_HEADER}" \
            "${BASE_URL}/health" \
            > /dev/null &
    done

    # Запросы к API с обращением в PostgreSQL.
    for _ in $(seq 1 10); do
        curl \
            --max-time 5 \
            -sS \
            -H "Host: ${HOST_HEADER}" \
            "${BASE_URL}/user" \
            > /dev/null &
    done

    # Медленные ответы для latency и алерта.
    for _ in $(seq 1 4); do
        curl \
            --max-time 10 \
            -sS \
            -H "Host: ${HOST_HEADER}" \
            "${BASE_URL}/debug/slow?delay_ms=750" \
            > /dev/null &
    done

    # Контролируемые 500-е ответы.
    for _ in $(seq 1 2); do
        curl \
            --max-time 5 \
            -sS \
            -H "Host: ${HOST_HEADER}" \
            "${BASE_URL}/debug/error" \
            > /dev/null &
    done

    wait

    if (( CYCLE % 10 == 0 )); then
        REMAINING=$((END_TIME - SECONDS))

        if (( REMAINING < 0 )); then
            REMAINING=0
        fi

        echo \
            "Цикл: ${CYCLE}; осталось примерно: ${REMAINING} сек."
    fi

    sleep 1
done

echo "Стресс-тест завершён"
