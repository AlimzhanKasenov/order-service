#!/usr/bin/env python3

import base64
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


GRAFANA_URL = os.getenv(
    "GRAFANA_URL",
    "http://127.0.0.1:13000",
).rstrip("/")

GRAFANA_USER = os.getenv(
    "GRAFANA_USER",
    "admin",
)

GRAFANA_PASSWORD = os.getenv("GRAFANA_PASSWORD")

FOLDER_UID = "order-service-alerts"
FOLDER_TITLE = "Order Service Alerts"
RULE_GROUP = "Order Service Monitoring"


if not GRAFANA_PASSWORD:
    raise SystemExit(
        "Не задан GRAFANA_PASSWORD"
    )


def api_request(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    allow_not_found: bool = False,
) -> tuple[int, Any]:
    url = f"{GRAFANA_URL}{path}"

    token = base64.b64encode(
        f"{GRAFANA_USER}:{GRAFANA_PASSWORD}".encode()
    ).decode()

    headers = {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "X-Disable-Provenance": "true",
    }

    data = None

    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(
        url=url,
        data=data,
        headers=headers,
        method=method,
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=30,
        ) as response:
            raw = response.read().decode("utf-8")

            if not raw:
                return response.status, None

            return response.status, json.loads(raw)

    except urllib.error.HTTPError as error:
        raw = error.read().decode(
            "utf-8",
            errors="replace",
        )

        if allow_not_found and error.code == 404:
            return error.code, None

        raise RuntimeError(
            f"{method} {path}: HTTP {error.code}: {raw}"
        ) from error


def get_prometheus_uid() -> str:
    _, datasources = api_request(
        "GET",
        "/api/datasources",
    )

    for datasource in datasources:
        if datasource.get("type") == "prometheus":
            return datasource["uid"]

    raise RuntimeError(
        "Prometheus datasource не найден"
    )


def ensure_folder() -> None:
    status, _ = api_request(
        "GET",
        f"/api/folders/{FOLDER_UID}",
        allow_not_found=True,
    )

    if status == 200:
        print(
            f"Folder уже существует: {FOLDER_TITLE}"
        )
        return

    api_request(
        "POST",
        "/api/folders",
        {
            "uid": FOLDER_UID,
            "title": FOLDER_TITLE,
        },
    )

    print(
        f"Folder создан: {FOLDER_TITLE}"
    )


def prometheus_query(
    ref_id: str,
    datasource_uid: str,
    expression: str,
) -> dict[str, Any]:
    return {
        "refId": ref_id,
        "queryType": "",
        "relativeTimeRange": {
            "from": 600,
            "to": 0,
        },
        "datasourceUid": datasource_uid,
        "model": {
            "datasource": {
                "type": "prometheus",
                "uid": datasource_uid,
            },
            "editorMode": "code",
            "expr": expression,
            "hide": False,
            "instant": True,
            "intervalMs": 1000,
            "maxDataPoints": 43200,
            "range": False,
            "refId": ref_id,
        },
    }


def threshold_condition(
    ref_id: str,
    query_ref_id: str,
    threshold: float,
) -> dict[str, Any]:
    return {
        "refId": ref_id,
        "queryType": "",
        "relativeTimeRange": {
            "from": 0,
            "to": 0,
        },
        "datasourceUid": "-100",
        "model": {
            "conditions": [
                {
                    "evaluator": {
                        "params": [
                            threshold,
                        ],
                        "type": "gt",
                    },
                    "operator": {
                        "type": "and",
                    },
                    "query": {
                        "params": [
                            query_ref_id,
                        ],
                    },
                    "reducer": {
                        "params": [],
                        "type": "last",
                    },
                    "type": "query",
                }
            ],
            "datasource": {
                "type": "__expr__",
                "uid": "-100",
            },
            "hide": False,
            "intervalMs": 1000,
            "maxDataPoints": 43200,
            "refId": ref_id,
            "type": "classic_conditions",
        },
    }


def make_rule(
    uid: str,
    title: str,
    expression: str,
    threshold: float,
    summary: str,
    description: str,
    datasource_uid: str,
) -> dict[str, Any]:
    return {
        "uid": uid,
        "title": title,
        "ruleGroup": RULE_GROUP,
        "folderUID": FOLDER_UID,
        "orgId": 1,
        "condition": "B",
        "noDataState": "OK",
        "execErrState": "Error",
        "for": "30s",
        "annotations": {
            "summary": summary,
            "description": description,
        },
        "labels": {
            "service": "order-service",
            "severity": "warning",
            "environment": "minikube",
        },
        "data": [
            prometheus_query(
                "A",
                datasource_uid,
                expression,
            ),
            threshold_condition(
                "B",
                "A",
                threshold,
            ),
        ],
    }


def upsert_rule(rule: dict[str, Any]) -> None:
    uid = rule["uid"]

    status, _ = api_request(
        "GET",
        f"/api/v1/provisioning/alert-rules/{uid}",
        allow_not_found=True,
    )

    if status == 200:
        api_request(
            "PUT",
            f"/api/v1/provisioning/alert-rules/{uid}",
            rule,
        )

        print(
            f"Alert обновлён: {rule['title']}"
        )
        return

    api_request(
        "POST",
        "/api/v1/provisioning/alert-rules",
        rule,
    )

    print(
        f"Alert создан: {rule['title']}"
    )


def main() -> None:
    datasource_uid = get_prometheus_uid()

    print(
        f"Prometheus datasource UID: {datasource_uid}"
    )

    ensure_folder()

    rules = [
        make_rule(
            uid="order-service-high-error-rate",
            title="Order Service — High Error Rate",
            expression=(
                "sum(increase("
                "order_service_http_requests_total"
                '{status=~"5.."}[1m]'
                "))"
            ),
            threshold=5,
            summary=(
                "В order-service обнаружено много ответов 5xx"
            ),
            description=(
                "За последнюю минуту сервис вернул "
                "более 5 ответов с кодом 5xx."
            ),
            datasource_uid=datasource_uid,
        ),
        make_rule(
            uid="order-service-high-latency",
            title="Order Service — High p95 Latency",
            expression=(
                "histogram_quantile("
                "0.95, "
                "sum by (le) ("
                "rate("
                "order_service_http_"
                "request_duration_seconds_bucket"
                "[1m]"
                ")"
                ")"
                ")"
            ),
            threshold=0.5,
            summary=(
                "p95 latency order-service выше 500 ms"
            ),
            description=(
                "p95 времени ответа сервиса "
                "превысил 0.5 секунды."
            ),
            datasource_uid=datasource_uid,
        ),
    ]

    for rule in rules:
        upsert_rule(rule)

    _, all_rules = api_request(
        "GET",
        "/api/v1/provisioning/alert-rules",
    )

    our_uids = {
        rule["uid"]
        for rule in rules
    }

    exported_rules = [
        rule
        for rule in all_rules
        if rule.get("uid") in our_uids
    ]

    output = Path(
        "grafana/order-service-alert-rules.json"
    )

    output.write_text(
        json.dumps(
            exported_rules,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        f"Alert rules сохранены: {output}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(
            f"Ошибка: {error}",
            file=sys.stderr,
        )
        raise
