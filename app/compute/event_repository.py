from __future__ import annotations

import json
import sqlite3
from typing import Any

EVENT_TASK_SUBMITTED = "task_submitted"
EVENT_TASK_CLAIMED = "task_claimed"
EVENT_LEASE_RENEWED = "lease_renewed"
EVENT_TASK_COMPLETED = "task_completed"
EVENT_TASK_FAILED = "task_failed"
EVENT_TASK_RECOVERED = "task_recovered"
EVENT_TASK_INTERVENED = "task_intervened"

EVENT_TYPES = (
    EVENT_TASK_SUBMITTED,
    EVENT_TASK_CLAIMED,
    EVENT_LEASE_RENEWED,
    EVENT_TASK_COMPLETED,
    EVENT_TASK_FAILED,
    EVENT_TASK_RECOVERED,
    EVENT_TASK_INTERVENED,
)


def _in_clause(column: str, values: list[str]) -> tuple[str, list[str]]:
    placeholders = ",".join("?" for _ in values)
    return f"{column} IN ({placeholders})", list(values)


def _match_clause(types: list[str], projects: list[str]) -> tuple[str, list[Any]]:
    """按事件类型和项目过滤；空列表表示不过滤。"""
    clauses: list[str] = []
    params: list[Any] = []
    if types:
        clause, values = _in_clause("e.event_type", types)
        clauses.append(clause)
        params.extend(values)
    if projects:
        clause, values = _in_clause("e.project_code", projects)
        clauses.append(clause)
        params.extend(values)
    return (" AND ".join(clauses), params)


# 未终结 = 没有已确认或已死信的投递记录；租约过期视为可再次领取
_UNRESOLVED = (
    "NOT EXISTS (SELECT 1 FROM compute_event_deliveries d "
    "WHERE d.subscription_id=? AND d.event_id=e.id AND d.status IN ('acked','dead'))"
)
_CLAIMABLE = (
    "NOT EXISTS (SELECT 1 FROM compute_event_deliveries d "
    "WHERE d.subscription_id=? AND d.event_id=e.id "
    "AND (d.status IN ('acked','dead') OR (d.status='leased' AND d.lease_expires_at>?)))"
)


class EventRepository:
    """封装运营事件流、订阅游标和投递状态的 SQLite 读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    # ---- 事件流（只写一次，之后不可变） ----

    def insert_event(self, *, event_key: str, event_type: str, task_id: int, task_version: int, project_code: str, occurred_at: str, payload: dict[str, Any], now: str) -> int:
        cursor = self.connection.execute(
            "INSERT INTO compute_events(event_key,event_type,task_id,task_version,project_code,occurred_at,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (event_key, event_type, task_id, task_version, project_code, occurred_at, json.dumps(payload, ensure_ascii=False, sort_keys=True), now),
        )
        return int(cursor.lastrowid)

    def event_by_id(self, event_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_events WHERE id=?", (event_id,)).fetchone()

    def latest_event_id(self) -> int:
        return int(self.connection.execute("SELECT COALESCE(MAX(id),0) FROM compute_events").fetchone()[0])

    def list_events(self, *, project_code: str | None, event_type: str | None, task_id: int | None, after_id: int, limit: int) -> list[sqlite3.Row]:
        clauses = ["id>?"]
        params: list[Any] = [after_id]
        if project_code:
            clauses.append("project_code=?")
            params.append(project_code)
        if event_type:
            clauses.append("event_type=?")
            params.append(event_type)
        if task_id is not None:
            clauses.append("task_id=?")
            params.append(task_id)
        params.append(limit)
        return self.connection.execute("SELECT * FROM compute_events WHERE " + " AND ".join(clauses) + " ORDER BY id LIMIT ?", params).fetchall()

    # ---- 订阅与游标 ----

    def subscription_by_code(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_event_subscriptions WHERE code=?", (code,)).fetchone()

    def list_subscriptions(self) -> list[sqlite3.Row]:
        return self.connection.execute("SELECT * FROM compute_event_subscriptions ORDER BY code").fetchall()

    def insert_subscription(self, *, code: str, description: str, event_types: list[str], project_codes: list[str], lease_seconds: int, max_delivery_attempts: int, cursor: int, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_event_subscriptions(code,description,event_types_json,project_codes_json,last_ack_event_id,lease_seconds,max_delivery_attempts,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (code, description, json.dumps(event_types, ensure_ascii=False), json.dumps(project_codes, ensure_ascii=False), cursor, lease_seconds, max_delivery_attempts, now, now),
        )

    def update_subscription(self, *, subscription_id: int, description: str, event_types: list[str], project_codes: list[str], lease_seconds: int, max_delivery_attempts: int, now: str) -> None:
        self.connection.execute(
            "UPDATE compute_event_subscriptions SET description=?,event_types_json=?,project_codes_json=?,lease_seconds=?,max_delivery_attempts=?,updated_at=? WHERE id=?",
            (description, json.dumps(event_types, ensure_ascii=False), json.dumps(project_codes, ensure_ascii=False), lease_seconds, max_delivery_attempts, now, subscription_id),
        )

    def advance_cursor(self, subscription_id: int, cursor: int, now: str) -> None:
        self.connection.execute(
            "UPDATE compute_event_subscriptions SET last_ack_event_id=?,updated_at=? WHERE id=? AND last_ack_event_id<?",
            (cursor, now, subscription_id, cursor),
        )

    def set_cursor(self, subscription_id: int, cursor: int, now: str) -> None:
        self.connection.execute(
            "UPDATE compute_event_subscriptions SET last_ack_event_id=?,updated_at=? WHERE id=?",
            (cursor, now, subscription_id),
        )

    # ---- 投递：领取、确认、失败、死信 ----

    def claimable_events(self, *, subscription_id: int, types: list[str], projects: list[str], now: str, limit: int) -> list[sqlite3.Row]:
        match, params = _match_clause(types, projects)
        clauses = [_CLAIMABLE]
        values: list[Any] = [subscription_id, now]
        if match:
            clauses.append(match)
            values.extend(params)
        values.append(limit)
        return self.connection.execute(
            "SELECT e.* FROM compute_events e WHERE " + " AND ".join(clauses) + " ORDER BY e.id LIMIT ?",
            values,
        ).fetchall()

    def lease_delivery(self, *, subscription_id: int, event_id: int, consumer: str, expires_at: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_event_deliveries(subscription_id,event_id,status,attempts,lease_owner,lease_expires_at,first_attempt_at,created_at,updated_at) VALUES(?,?,'leased',1,?,?,?,?,?) "
            "ON CONFLICT(subscription_id,event_id) DO UPDATE SET status='leased',attempts=attempts+1,lease_owner=excluded.lease_owner,lease_expires_at=excluded.lease_expires_at,updated_at=excluded.updated_at",
            (subscription_id, event_id, consumer, expires_at, now, now, now),
        )

    def deaden_exhausted_leases(self, *, subscription_id: int, now: str, max_attempts: int) -> int:
        cursor = self.connection.execute(
            "UPDATE compute_event_deliveries SET status='dead',dead_reason='租约多次过期且达到最大投递次数',lease_owner='',lease_expires_at='',updated_at=? WHERE subscription_id=? AND status='leased' AND lease_expires_at<=? AND attempts>=?",
            (now, subscription_id, now, max_attempts),
        )
        return int(cursor.rowcount)

    def delivery_by_event(self, subscription_id: int, event_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM compute_event_deliveries WHERE subscription_id=? AND event_id=?",
            (subscription_id, event_id),
        ).fetchone()

    def ack_delivery(self, delivery_id: int, now: str) -> None:
        self.connection.execute(
            "UPDATE compute_event_deliveries SET status='acked',lease_owner='',lease_expires_at='',acked_at=?,updated_at=? WHERE id=?",
            (now, now, delivery_id),
        )

    def release_delivery(self, delivery_id: int, *, error: str, dead: bool, now: str) -> None:
        if dead:
            self.connection.execute(
                "UPDATE compute_event_deliveries SET status='dead',dead_reason=?,last_error=?,lease_owner='',lease_expires_at='',updated_at=? WHERE id=?",
                (error, error, now, delivery_id),
            )
        else:
            self.connection.execute(
                "UPDATE compute_event_deliveries SET status='pending',last_error=?,lease_owner='',lease_expires_at='',updated_at=? WHERE id=?",
                (error, now, delivery_id),
            )

    def first_unresolved_event_id(self, *, subscription_id: int, after_id: int, types: list[str], projects: list[str]) -> int | None:
        match, params = _match_clause(types, projects)
        clauses = ["e.id>?", _UNRESOLVED]
        values: list[Any] = [after_id, subscription_id]
        if match:
            clauses.append(match)
            values.extend(params)
        row = self.connection.execute(
            "SELECT MIN(e.id) FROM compute_events e WHERE " + " AND ".join(clauses),
            values,
        ).fetchone()
        return None if row is None or row[0] is None else int(row[0])

    def count_unresolved(self, *, subscription_id: int, types: list[str], projects: list[str]) -> tuple[int, str | None]:
        match, params = _match_clause(types, projects)
        clauses = [_UNRESOLVED]
        values: list[Any] = [subscription_id]
        if match:
            clauses.append(match)
            values.extend(params)
        row = self.connection.execute(
            "SELECT COUNT(*),MIN(e.occurred_at) FROM compute_events e WHERE " + " AND ".join(clauses),
            values,
        ).fetchone()
        return int(row[0]), (str(row[1]) if row[1] else None)

    def delivery_totals(self, subscription_id: int) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT status,COUNT(*) AS amount,COALESCE(SUM(attempts),0) AS attempts FROM compute_event_deliveries WHERE subscription_id=? GROUP BY status",
            (subscription_id,),
        ).fetchall()
        by_status = {str(row["status"]): int(row["amount"]) for row in rows}
        attempts = sum(int(row["attempts"]) for row in rows)
        failed = int(self.connection.execute("SELECT COUNT(*) FROM compute_event_deliveries WHERE subscription_id=? AND last_error<>''", (subscription_id,)).fetchone()[0])
        requeued = int(self.connection.execute("SELECT COALESCE(SUM(requeue_count),0) FROM compute_event_deliveries WHERE subscription_id=?", (subscription_id,)).fetchone()[0])
        return {"by_status": by_status, "attempts": attempts, "failed": failed, "requeued": requeued}

    def dead_letters(self, subscription_id: int) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT d.*,e.event_type,e.task_id,e.task_version,e.project_code,e.occurred_at FROM compute_event_deliveries d JOIN compute_events e ON e.id=d.event_id WHERE d.subscription_id=? AND d.status='dead' ORDER BY d.event_id",
            (subscription_id,),
        ).fetchall()

    def requeue_dead(self, *, subscription_id: int, event_ids: list[int], now: str) -> list[int]:
        clause = ""
        params: list[Any] = [subscription_id]
        if event_ids:
            placeholders = ",".join("?" for _ in event_ids)
            clause = f" AND event_id IN ({placeholders})"
            params.extend(event_ids)
        rows = self.connection.execute(
            "SELECT event_id FROM compute_event_deliveries WHERE subscription_id=? AND status='dead'" + clause,
            params,
        ).fetchall()
        ids = [int(row["event_id"]) for row in rows]
        if ids:
            placeholders = ",".join("?" for _ in ids)
            self.connection.execute(
                f"UPDATE compute_event_deliveries SET status='pending',attempts=0,lease_owner='',lease_expires_at='',requeue_count=requeue_count+1,updated_at=? WHERE subscription_id=? AND event_id IN ({placeholders})",
                [now, subscription_id, *ids],
            )
        return ids
