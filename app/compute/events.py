from __future__ import annotations

import json
import sqlite3
from datetime import timedelta
from typing import Any

from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import NotFoundError, ValidationError
from app.database import get_connection, transaction

EVENT_TYPES = (
    "task_submitted",
    "task_claimed",
    "lease_renewed",
    "task_completed",
    "task_failed",
    "lease_recovered",
    "manual_intervention",
)

EVENT_TYPE_SET = frozenset(EVENT_TYPES)


def emit_task_event(connection: sqlite3.Connection, *, event_type: str, task: dict[str, Any], now: str, extra: dict[str, Any] | None = None) -> None:
    """在业务变更的同一事务内追加一条不可变运营事件。

    事件唯一键由任务、事件类型和任务版本号组成：每次业务变更恰好推进一次版本，
    因此同一业务变化无论重试多少次都只会留下一个稳定事件。
    """
    payload: dict[str, Any] = {
        "task_id": task["id"],
        "project_code": task["project_code"],
        "requested_by": task["requested_by"],
        "template_code": task["template_code"],
        "status": task["status"],
        "priority": task["priority"],
        "attempt_count": task["attempt_count"],
        "task_version": task["version"],
    }
    if extra:
        payload.update(extra)
    event_uid = f"task:{task['id']}:{event_type}:{task['version']}"
    connection.execute(
        "INSERT OR IGNORE INTO compute_events(event_uid,event_type,task_id,project_code,task_version,payload_json,created_at) VALUES(?,?,?,?,?,?,?)",
        (event_uid, event_type, task["id"], task["project_code"], task["version"], json.dumps(payload, ensure_ascii=False, sort_keys=True), now),
    )


def _event_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "event_id": row["id"],
        "event_uid": row["event_uid"],
        "event_type": row["event_type"],
        "task_id": row["task_id"],
        "project_code": row["project_code"],
        "task_version": row["task_version"],
        "payload": json.loads(row["payload_json"]),
        "created_at": row["created_at"],
    }


class ComputeEventService:
    """为值班交接、内部计费等订阅者提供可恢复、可过滤、保序的运营事件消费。

    语义约定：
    - 事件流全局按 id 递增排序，订阅者按游标顺序批量领取；存在未确认的在途事件时，
      后续事件不会被领取，因此积压再大也不会打乱全局顺序和任务内因果顺序。
    - 租约超时未确认的事件会自动重投；超过最大投递次数进入死信，游标越过死信继续前进。
    - 游标只向前移动，重启后从最后确认位置继续；重新投入死信只回退游标和投递状态，不改写原事件。
    - 更新订阅的过滤条件不会重置游标：已经越过的事件不会重新投递。
    """

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()

    # ---------- 订阅管理 ----------

    def upsert_subscription(self, name: str, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        unknown = sorted(set(payload["event_types"]) - EVENT_TYPE_SET)
        if unknown:
            raise ValidationError("包含未知事件类型", context={"event_types": unknown})
        now = to_storage(self.clock.now())
        project_code = payload.get("project_code") or ""
        event_types_json = json.dumps(sorted(set(payload["event_types"])), ensure_ascii=False)
        with transaction(immediate=True) as connection:
            connection.execute(
                """
                INSERT INTO compute_event_subscriptions(name,description,project_code,event_types_json,lease_seconds,max_attempts,max_batch_size,cursor_event_id,created_by,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,0,?,?,?)
                ON CONFLICT(name) DO UPDATE SET description=excluded.description,project_code=excluded.project_code,event_types_json=excluded.event_types_json,lease_seconds=excluded.lease_seconds,max_attempts=excluded.max_attempts,max_batch_size=excluded.max_batch_size,updated_at=excluded.updated_at
                """,
                (name, payload["description"], project_code, event_types_json, payload["lease_seconds"], payload["max_attempts"], payload["max_batch_size"], actor, now, now),
            )
            return self._subscription_view(connection, self._subscription(connection, name), now)

    def list_subscriptions(self) -> list[dict[str, Any]]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            names = [row["name"] for row in connection.execute("SELECT name FROM compute_event_subscriptions ORDER BY name").fetchall()]
            views: list[dict[str, Any]] = []
            for name in names:
                subscription = self._subscription(connection, name)
                self._sweep_expired(connection, subscription, now)
                self._advance_cursor(connection, subscription, now)
                views.append(self._subscription_view(connection, self._subscription(connection, name), now))
            return views

    def get_subscription(self, name: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            subscription = self._subscription(connection, name)
            self._sweep_expired(connection, subscription, now)
            self._advance_cursor(connection, subscription, now)
            return self._subscription_view(connection, self._subscription(connection, name), now)

    # ---------- 领取、确认与重投 ----------

    def claim(self, name: str, *, limit: int | None = None, consumer: str = "") -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            subscription = self._subscription(connection, name)
            self._sweep_expired(connection, subscription, now)
            cursor = self._advance_cursor(connection, subscription, now)
            filter_sql, filter_params = self._filter(subscription)
            blocker = connection.execute(
                f"SELECT MIN(d.event_id) FROM compute_event_deliveries d JOIN compute_events e ON e.id=d.event_id WHERE d.subscription_id=? AND d.status='claimed' AND d.lease_expires_at>? AND d.event_id>? AND {filter_sql}",
                (subscription["id"], now, cursor, *filter_params),
            ).fetchone()[0]
            batch_limit = int(subscription["max_batch_size"]) if limit is None else max(1, min(limit, int(subscription["max_batch_size"])))
            sql = f"SELECT e.* FROM compute_events e WHERE e.id>? AND {filter_sql} AND NOT EXISTS (SELECT 1 FROM compute_event_deliveries d WHERE d.subscription_id=? AND d.event_id=e.id AND d.status IN ('acked','dead'))"
            params: list[Any] = [cursor, *filter_params, subscription["id"]]
            if blocker is not None:
                sql += " AND e.id<?"
                params.append(blocker)
            sql += " ORDER BY e.id LIMIT ?"
            params.append(batch_limit)
            rows = connection.execute(sql, params).fetchall()
            lease_until = to_storage(now_value + timedelta(seconds=int(subscription["lease_seconds"])))
            items: list[dict[str, Any]] = []
            for row in rows:
                attempt = self._mark_claimed(connection, subscription, int(row["id"]), consumer, now, lease_until)
                item = _event_dict(row)
                item["attempt"] = attempt
                item["lease_expires_at"] = lease_until
                items.append(item)
            return {"subscription": name, "cursor_event_id": cursor, "claimed_by": consumer, "items": items}

    def ack(self, name: str, event_ids: list[int]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            subscription = self._subscription(connection, name)
            acked: list[int] = []
            skipped: list[dict[str, Any]] = []
            for event_id in dict.fromkeys(event_ids):
                delivery = connection.execute("SELECT * FROM compute_event_deliveries WHERE subscription_id=? AND event_id=?", (subscription["id"], event_id)).fetchone()
                if delivery is None:
                    skipped.append({"event_id": event_id, "reason": "not_claimed"})
                elif delivery["status"] == "acked":
                    skipped.append({"event_id": event_id, "reason": "already_acked"})
                elif delivery["status"] == "dead":
                    skipped.append({"event_id": event_id, "reason": "dead"})
                else:
                    connection.execute("UPDATE compute_event_deliveries SET status='acked',acked_at=?,lease_expires_at='' WHERE id=?", (now, delivery["id"]))
                    acked.append(event_id)
            cursor = self._advance_cursor(connection, subscription, now)
            return {"subscription": name, "acked": acked, "skipped": skipped, "cursor_event_id": cursor}

    def nack(self, name: str, event_ids: list[int], reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        reason = reason[:500]
        with transaction(immediate=True) as connection:
            subscription = self._subscription(connection, name)
            released: list[int] = []
            dead: list[int] = []
            skipped: list[dict[str, Any]] = []
            for event_id in dict.fromkeys(event_ids):
                delivery = connection.execute("SELECT * FROM compute_event_deliveries WHERE subscription_id=? AND event_id=?", (subscription["id"], event_id)).fetchone()
                if delivery is None:
                    skipped.append({"event_id": event_id, "reason": "not_claimed"})
                    continue
                if delivery["status"] in {"acked", "dead"}:
                    skipped.append({"event_id": event_id, "reason": delivery["status"]})
                    continue
                if delivery["status"] == "released":
                    skipped.append({"event_id": event_id, "reason": "already_released"})
                    continue
                if int(delivery["attempts"]) >= int(subscription["max_attempts"]):
                    connection.execute(
                        "UPDATE compute_event_deliveries SET status='dead',failure_count=failure_count+1,last_error=?,dead_reason=?,dead_at=?,lease_expires_at='' WHERE id=?",
                        (reason, reason, now, delivery["id"]),
                    )
                    dead.append(event_id)
                else:
                    connection.execute(
                        "UPDATE compute_event_deliveries SET status='released',failure_count=failure_count+1,last_error=?,lease_expires_at='' WHERE id=?",
                        (reason, delivery["id"]),
                    )
                    released.append(event_id)
            cursor = self._advance_cursor(connection, subscription, now)
            return {"subscription": name, "released": released, "dead": dead, "skipped": skipped, "cursor_event_id": cursor}

    # ---------- 死信与事件检查 ----------

    def dead_letters(self, name: str, limit: int = 100) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            subscription = self._subscription(connection, name)
            self._sweep_expired(connection, subscription, now)
            rows = connection.execute(
                "SELECT d.*,e.event_uid,e.event_type,e.task_id,e.project_code,e.task_version,e.payload_json,e.created_at AS event_created_at FROM compute_event_deliveries d JOIN compute_events e ON e.id=d.event_id WHERE d.subscription_id=? AND d.status='dead' ORDER BY d.event_id LIMIT ?",
                (subscription["id"], max(1, min(limit, 500))),
            ).fetchall()
            items = [
                {
                    "event_id": row["event_id"],
                    "event_uid": row["event_uid"],
                    "event_type": row["event_type"],
                    "task_id": row["task_id"],
                    "project_code": row["project_code"],
                    "attempts": row["attempts"],
                    "failure_count": row["failure_count"],
                    "requeue_count": row["requeue_count"],
                    "dead_reason": row["dead_reason"],
                    "dead_at": row["dead_at"],
                    "last_error": row["last_error"],
                    "last_requeued_by": row["last_requeued_by"],
                    "last_requeued_at": row["last_requeued_at"],
                    "payload": json.loads(row["payload_json"]),
                    "event_created_at": row["event_created_at"],
                }
                for row in rows
            ]
            return {"subscription": name, "items": items}

    def requeue_dead_letters(self, name: str, event_ids: list[int] | None, actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            subscription = self._subscription(connection, name)
            self._sweep_expired(connection, subscription, now)
            skipped: list[dict[str, Any]] = []
            if event_ids is None:
                rows = connection.execute("SELECT * FROM compute_event_deliveries WHERE subscription_id=? AND status='dead' ORDER BY event_id", (subscription["id"],)).fetchall()
            else:
                rows = []
                for event_id in dict.fromkeys(event_ids):
                    row = connection.execute("SELECT * FROM compute_event_deliveries WHERE subscription_id=? AND event_id=?", (subscription["id"], event_id)).fetchone()
                    if row is None:
                        skipped.append({"event_id": event_id, "reason": "not_claimed"})
                    elif row["status"] != "dead":
                        skipped.append({"event_id": event_id, "reason": row["status"]})
                    else:
                        rows.append(row)
            requeued: list[int] = []
            for row in rows:
                connection.execute(
                    "UPDATE compute_event_deliveries SET status='released',attempts=0,lease_expires_at='',claimed_by='',requeue_count=requeue_count+1,last_requeued_by=?,last_requeued_at=? WHERE id=?",
                    (actor, now, row["id"]),
                )
                requeued.append(int(row["event_id"]))
            cursor = int(subscription["cursor_event_id"])
            if requeued:
                cursor = min(cursor, min(requeued) - 1)
                connection.execute("UPDATE compute_event_subscriptions SET cursor_event_id=?,updated_at=? WHERE id=?", (cursor, now, subscription["id"]))
            return {"subscription": name, "requeued": requeued, "skipped": skipped, "cursor_event_id": cursor}

    def list_events(self, *, project_code: str | None = None, event_type: str | None = None, task_id: int | None = None, since_id: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        clauses = ["e.id>?"]
        params: list[Any] = [max(0, since_id)]
        if project_code:
            clauses.append("e.project_code=?")
            params.append(project_code)
        if event_type:
            if event_type not in EVENT_TYPE_SET:
                raise ValidationError("未知事件类型", context={"event_type": event_type})
            clauses.append("e.event_type=?")
            params.append(event_type)
        if task_id is not None:
            clauses.append("e.task_id=?")
            params.append(task_id)
        params.append(max(1, min(limit, 500)))
        rows = self.connection.execute(f"SELECT e.* FROM compute_events e WHERE {' AND '.join(clauses)} ORDER BY e.id LIMIT ?", params).fetchall()
        return [_event_dict(row) for row in rows]

    # ---------- 内部辅助 ----------

    def _subscription(self, connection: sqlite3.Connection, name: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM compute_event_subscriptions WHERE name=?", (name,)).fetchone()
        if row is None:
            raise NotFoundError("事件订阅不存在")
        return row

    @staticmethod
    def _filter(subscription: sqlite3.Row) -> tuple[str, list[Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if subscription["project_code"]:
            clauses.append("e.project_code=?")
            params.append(subscription["project_code"])
        event_types = json.loads(subscription["event_types_json"])
        if event_types:
            placeholders = ",".join("?" for _ in event_types)
            clauses.append(f"e.event_type IN ({placeholders})")
            params.extend(event_types)
        return (" AND ".join(clauses) if clauses else "1=1"), params

    @staticmethod
    def _mark_claimed(connection: sqlite3.Connection, subscription: sqlite3.Row, event_id: int, consumer: str, now: str, lease_until: str) -> int:
        delivery = connection.execute("SELECT * FROM compute_event_deliveries WHERE subscription_id=? AND event_id=?", (subscription["id"], event_id)).fetchone()
        if delivery is None:
            connection.execute(
                "INSERT INTO compute_event_deliveries(subscription_id,event_id,status,attempts,claimed_by,first_claimed_at,last_claimed_at,lease_expires_at) VALUES(?,?, 'claimed',1,?,?,?,?)",
                (subscription["id"], event_id, consumer, now, now, lease_until),
            )
            return 1
        attempt = int(delivery["attempts"]) + 1
        if delivery["status"] == "released":
            connection.execute(
                "UPDATE compute_event_deliveries SET status='claimed',attempts=?,claimed_by=?,last_claimed_at=?,lease_expires_at=? WHERE id=?",
                (attempt, consumer, now, lease_until, delivery["id"]),
            )
        else:
            connection.execute(
                "UPDATE compute_event_deliveries SET status='claimed',attempts=?,failure_count=failure_count+1,last_error='lease_timeout',claimed_by=?,last_claimed_at=?,lease_expires_at=? WHERE id=?",
                (attempt, consumer, now, lease_until, delivery["id"]),
            )
        return attempt

    @staticmethod
    def _sweep_expired(connection: sqlite3.Connection, subscription: sqlite3.Row, now: str) -> None:
        connection.execute(
            "UPDATE compute_event_deliveries SET status='dead',failure_count=failure_count+1,last_error='attempts_exhausted',dead_reason='attempts_exhausted',dead_at=?,lease_expires_at='' WHERE subscription_id=? AND status='claimed' AND lease_expires_at<>'' AND lease_expires_at<=? AND attempts>=?",
            (now, subscription["id"], now, subscription["max_attempts"]),
        )

    def _advance_cursor(self, connection: sqlite3.Connection, subscription: sqlite3.Row, now: str) -> int:
        cursor = int(subscription["cursor_event_id"])
        filter_sql, filter_params = self._filter(subscription)
        first_open = connection.execute(
            f"SELECT MIN(e.id) FROM compute_events e WHERE e.id>? AND {filter_sql} AND NOT EXISTS (SELECT 1 FROM compute_event_deliveries d WHERE d.subscription_id=? AND d.event_id=e.id AND d.status IN ('acked','dead'))",
            (cursor, *filter_params, subscription["id"]),
        ).fetchone()[0]
        if first_open is None:
            latest = connection.execute("SELECT COALESCE(MAX(id),0) FROM compute_events").fetchone()[0]
            new_cursor = max(cursor, int(latest))
        else:
            new_cursor = int(first_open) - 1
        if new_cursor > cursor:
            connection.execute("UPDATE compute_event_subscriptions SET cursor_event_id=?,updated_at=? WHERE id=?", (new_cursor, now, subscription["id"]))
        return new_cursor

    def _subscription_view(self, connection: sqlite3.Connection, subscription: sqlite3.Row, now: str) -> dict[str, Any]:
        view = dict(subscription)
        view["event_types"] = json.loads(view.pop("event_types_json"))
        view["stats"] = self._stats(connection, subscription, now)
        return view

    def _stats(self, connection: sqlite3.Connection, subscription: sqlite3.Row, now: str) -> dict[str, Any]:
        filter_sql, filter_params = self._filter(subscription)
        cursor = int(subscription["cursor_event_id"])
        latest = int(connection.execute("SELECT COALESCE(MAX(id),0) FROM compute_events").fetchone()[0])
        open_row = connection.execute(
            f"SELECT COUNT(*) AS amount,MIN(e.created_at) AS oldest FROM compute_events e WHERE e.id>? AND {filter_sql} AND NOT EXISTS (SELECT 1 FROM compute_event_deliveries d WHERE d.subscription_id=? AND d.event_id=e.id AND d.status IN ('acked','dead'))",
            (cursor, *filter_params, subscription["id"]),
        ).fetchone()
        in_flight = int(
            connection.execute(
                f"SELECT COUNT(*) FROM compute_event_deliveries d JOIN compute_events e ON e.id=d.event_id WHERE d.subscription_id=? AND d.status='claimed' AND d.lease_expires_at>? AND {filter_sql}",
                (subscription["id"], now, *filter_params),
            ).fetchone()[0]
        )
        totals = connection.execute(
            "SELECT COALESCE(SUM(CASE WHEN status='acked' THEN 1 ELSE 0 END),0) AS acked,COALESCE(SUM(CASE WHEN status='dead' THEN 1 ELSE 0 END),0) AS dead,COALESCE(SUM(attempts),0) AS attempts,COALESCE(SUM(failure_count),0) AS failures,COALESCE(SUM(CASE WHEN attempts>1 THEN 1 ELSE 0 END),0) AS redelivered,COALESCE(SUM(requeue_count),0) AS requeues,MAX(last_claimed_at) AS last_claimed_at,MAX(acked_at) AS last_acked_at FROM compute_event_deliveries WHERE subscription_id=?",
            (subscription["id"],),
        ).fetchone()
        unprocessed = int(open_row["amount"])
        oldest = open_row["oldest"]
        lag_seconds = 0
        if oldest:
            oldest_at = from_storage(oldest)
            now_at = from_storage(now)
            if oldest_at and now_at:
                lag_seconds = max(0, int((now_at - oldest_at).total_seconds()))
        return {
            "cursor_event_id": cursor,
            "latest_event_id": latest,
            "unprocessed_events": unprocessed,
            "in_flight_events": in_flight,
            "awaiting_delivery_events": unprocessed - in_flight,
            "oldest_unprocessed_at": oldest,
            "lag_seconds": lag_seconds,
            "acked_events": int(totals["acked"]),
            "dead_events": int(totals["dead"]),
            "delivery_attempts": int(totals["attempts"]),
            "failures": int(totals["failures"]),
            "redelivered_events": int(totals["redelivered"]),
            "requeued_events": int(totals["requeues"]),
            "last_claimed_at": totals["last_claimed_at"],
            "last_acked_at": totals["last_acked_at"],
        }
