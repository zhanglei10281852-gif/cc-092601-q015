from __future__ import annotations

import json
import sqlite3
from datetime import timedelta
from typing import Any

from app.compute.event_repository import EVENT_TYPES, EventRepository
from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction


def record_task_event(connection: sqlite3.Connection, event_type: str, task: dict[str, Any], detail: dict[str, Any], now: str) -> int:
    """在业务事务内写入一条不可变运营事件。

    event_key 由事件类型、任务和变更后的任务版本组成；每次业务变更只会把任务版本
    推进一次，因此同一业务变化重放时得到同一个稳定键，唯一约束保证不会重复入账。
    """
    return EventRepository(connection).insert_event(
        event_key=f"{event_type}:{int(task['id'])}:{int(task['version'])}",
        event_type=event_type,
        task_id=int(task["id"]),
        task_version=int(task["version"]),
        project_code=str(task["project_code"]),
        occurred_at=now,
        payload={"task": task, "detail": detail},
        now=now,
    )


def _parse_filters(subscription: sqlite3.Row) -> tuple[list[str], list[str]]:
    types = json.loads(subscription["event_types_json"])
    projects = json.loads(subscription["project_codes_json"])
    return list(types), list(projects)


def _event_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "event_key": row["event_key"],
        "event_type": row["event_type"],
        "task_id": row["task_id"],
        "task_version": row["task_version"],
        "project_code": row["project_code"],
        "occurred_at": row["occurred_at"],
        "payload": json.loads(row["payload_json"]),
        "created_at": row["created_at"],
    }


class EventStreamService:
    """管理事件订阅、批量领取、确认、超时重投、死信与消费监控。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = EventRepository(self.connection)

    # ---- 订阅注册 ----

    def upsert_subscription(self, code: str, payload: dict[str, Any]) -> dict[str, Any]:
        types = sorted(set(payload["event_types"]))
        unknown = sorted(set(types) - set(EVENT_TYPES))
        if unknown:
            raise ValidationError("包含未知事件类型", context={"event_types": unknown})
        projects = sorted(set(payload["project_codes"]))
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = EventRepository(connection)
            existing = repository.subscription_by_code(code)
            if existing is None:
                cursor = 0 if payload["start_from"] == "beginning" else repository.latest_event_id()
                repository.insert_subscription(
                    code=code, description=payload["description"], event_types=types, project_codes=projects,
                    lease_seconds=payload["lease_seconds"], max_delivery_attempts=payload["max_delivery_attempts"],
                    cursor=cursor, now=now,
                )
            else:
                repository.update_subscription(
                    subscription_id=existing["id"], description=payload["description"], event_types=types,
                    project_codes=projects, lease_seconds=payload["lease_seconds"],
                    max_delivery_attempts=payload["max_delivery_attempts"], now=now,
                )
                if payload["reset_cursor"]:
                    cursor = 0 if payload["start_from"] == "beginning" else repository.latest_event_id()
                    repository.set_cursor(existing["id"], cursor, now)
            subscription = repository.subscription_by_code(code)
            return self._subscription_view(repository, subscription)

    def get_subscription(self, code: str) -> dict[str, Any]:
        subscription = self.repository.subscription_by_code(code)
        if subscription is None:
            raise NotFoundError("事件订阅不存在")
        return self._subscription_view(self.repository, subscription)

    def list_subscriptions(self) -> list[dict[str, Any]]:
        return [self._subscription_view(self.repository, row) for row in self.repository.list_subscriptions()]

    # ---- 消费：领取、确认、失败 ----

    def claim(self, code: str, consumer_id: str, batch_size: int, lease_seconds: int | None = None) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = EventRepository(connection)
            subscription = repository.subscription_by_code(code)
            if subscription is None:
                raise NotFoundError("事件订阅不存在")
            types, projects = _parse_filters(subscription)
            lease = lease_seconds or int(subscription["lease_seconds"])
            expires = to_storage(now_value + timedelta(seconds=lease))
            repository.deaden_exhausted_leases(subscription_id=subscription["id"], now=now, max_attempts=int(subscription["max_delivery_attempts"]))
            rows = repository.claimable_events(
                subscription_id=subscription["id"], types=types, projects=projects, now=now, limit=batch_size,
            )
            deliveries: list[dict[str, Any]] = []
            for row in rows:
                repository.lease_delivery(subscription_id=subscription["id"], event_id=row["id"], consumer=consumer_id, expires_at=expires, now=now)
                delivery = repository.delivery_by_event(subscription["id"], row["id"])
                deliveries.append({
                    "delivery_id": delivery["id"],
                    "event_id": row["id"],
                    "attempt": delivery["attempts"],
                    "lease_expires_at": expires,
                    "event": _event_payload(row),
                })
            return {"subscription": code, "consumer": consumer_id, "lease_expires_at": expires, "deliveries": deliveries}

    def ack(self, code: str, consumer_id: str, event_ids: list[int]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = EventRepository(connection)
            subscription = repository.subscription_by_code(code)
            if subscription is None:
                raise NotFoundError("事件订阅不存在")
            acked: list[int] = []
            for event_id in sorted(set(event_ids)):
                delivery = self._held_delivery(repository, subscription["id"], event_id, consumer_id)
                repository.ack_delivery(delivery["id"], now)
                acked.append(event_id)
            cursor = self._resolved_cursor(repository, subscription)
            repository.advance_cursor(subscription["id"], cursor, now)
            return {"subscription": code, "acked": acked, "last_ack_event_id": cursor}

    def nack(self, code: str, consumer_id: str, event_ids: list[int], error: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = EventRepository(connection)
            subscription = repository.subscription_by_code(code)
            if subscription is None:
                raise NotFoundError("事件订阅不存在")
            results: list[dict[str, Any]] = []
            for event_id in sorted(set(event_ids)):
                delivery = self._held_delivery(repository, subscription["id"], event_id, consumer_id)
                dead = int(delivery["attempts"]) >= int(subscription["max_delivery_attempts"])
                repository.release_delivery(delivery["id"], error=error[:2000], dead=dead, now=now)
                results.append({"event_id": event_id, "status": "dead" if dead else "pending"})
            cursor = self._resolved_cursor(repository, subscription)
            repository.advance_cursor(subscription["id"], cursor, now)
            return {"subscription": code, "results": results, "last_ack_event_id": cursor}

    # ---- 管理：事件查询、死信、监控 ----

    def list_events(self, *, project_code: str | None = None, event_type: str | None = None, task_id: int | None = None, after_id: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        if event_type and event_type not in EVENT_TYPES:
            raise ValidationError("未知事件类型", context={"event_type": event_type})
        rows = self.repository.list_events(
            project_code=project_code, event_type=event_type, task_id=task_id,
            after_id=max(0, after_id), limit=max(1, min(limit, 500)),
        )
        return [_event_payload(row) for row in rows]

    def dead_letters(self, code: str) -> list[dict[str, Any]]:
        subscription = self.repository.subscription_by_code(code)
        if subscription is None:
            raise NotFoundError("事件订阅不存在")
        return [
            {
                "delivery_id": row["id"],
                "event_id": row["event_id"],
                "event_type": row["event_type"],
                "task_id": row["task_id"],
                "task_version": row["task_version"],
                "project_code": row["project_code"],
                "occurred_at": row["occurred_at"],
                "attempts": row["attempts"],
                "dead_reason": row["dead_reason"],
                "last_error": row["last_error"],
                "requeue_count": row["requeue_count"],
                "first_attempt_at": row["first_attempt_at"],
                "updated_at": row["updated_at"],
            }
            for row in self.repository.dead_letters(subscription["id"])
        ]

    def requeue_dead(self, code: str, event_ids: list[int] | None = None) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = EventRepository(connection)
            subscription = repository.subscription_by_code(code)
            if subscription is None:
                raise NotFoundError("事件订阅不存在")
            requeued = repository.requeue_dead(subscription_id=subscription["id"], event_ids=sorted(set(event_ids or [])), now=now)
            return {"subscription": code, "requeued_event_ids": requeued, "requeued": len(requeued)}

    # ---- 内部 ----

    @staticmethod
    def _held_delivery(repository: EventRepository, subscription_id: int, event_id: int, consumer_id: str) -> sqlite3.Row:
        delivery = repository.delivery_by_event(subscription_id, event_id)
        if delivery is None or delivery["status"] != "leased" or delivery["lease_owner"] != consumer_id:
            raise ConflictError("投递不存在或未由当前消费者持有", context={"event_id": event_id})
        return delivery

    @staticmethod
    def _resolved_cursor(repository: EventRepository, subscription: sqlite3.Row) -> int:
        """游标只能向前推进到“之前事件全部已确认或已死信”的位置。"""
        types, projects = _parse_filters(subscription)
        gap = repository.first_unresolved_event_id(
            subscription_id=subscription["id"], after_id=int(subscription["last_ack_event_id"]),
            types=types, projects=projects,
        )
        if gap is None:
            return repository.latest_event_id()
        return gap - 1

    def _subscription_view(self, repository: EventRepository, subscription: sqlite3.Row) -> dict[str, Any]:
        types, projects = _parse_filters(subscription)
        pending, oldest_pending_at = repository.count_unresolved(subscription_id=subscription["id"], types=types, projects=projects)
        totals = repository.delivery_totals(subscription["id"])
        latest = repository.latest_event_id()
        lag_seconds = 0.0
        if oldest_pending_at:
            started = from_storage(oldest_pending_at)
            if started is not None:
                lag_seconds = max(0.0, (self.clock.now() - started).total_seconds())
        by_status = totals["by_status"]
        return {
            "code": subscription["code"],
            "description": subscription["description"],
            "event_types": types,
            "project_codes": projects,
            "lease_seconds": subscription["lease_seconds"],
            "max_delivery_attempts": subscription["max_delivery_attempts"],
            "cursor": {
                "last_ack_event_id": subscription["last_ack_event_id"],
                "latest_event_id": latest,
                "pending_events": pending,
                "oldest_pending_at": oldest_pending_at,
                "lag_seconds": lag_seconds,
            },
            "deliveries": {
                "leased": by_status.get("leased", 0),
                "acked": by_status.get("acked", 0),
                "dead": by_status.get("dead", 0),
                "attempts": totals["attempts"],
                "failed": totals["failed"],
                "requeued": totals["requeued"],
            },
            "created_at": subscription["created_at"],
            "updated_at": subscription["updated_at"],
        }
