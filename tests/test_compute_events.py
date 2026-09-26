from __future__ import annotations

from datetime import UTC, datetime

from app.compute.events import ComputeEventService
from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import get_connection, init_db

TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "tolerance": {"type": "number", "required": False, "minimum": 0.0, "maximum": 1.0},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {"tolerance": 0.001},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}


def submit_payload(key: str, *, user: str = "researcher-1", project: str = "project-a", priority: int = 50) -> dict:
    return {
        "template_code": "solver-a",
        "project_code": project,
        "requested_by": user,
        "parameters": {"iterations": 100, "mode": "accurate"},
        "priority": priority,
        "idempotency_key": key,
    }


def create_template(client) -> None:
    response = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert response.status_code == 201, response.text


def make_services() -> tuple[FrozenClock, ComputeOperationsService, ComputeEventService]:
    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    operations = ComputeOperationsService(get_connection(), clock)
    events = ComputeEventService(get_connection(), clock)
    return clock, operations, events


def subscribe(events: ComputeEventService, name: str, **overrides) -> dict:
    payload = {"description": "", "project_code": None, "event_types": [], "lease_seconds": 30, "max_attempts": 3, "max_batch_size": 10}
    payload.update(overrides)
    return events.upsert_subscription(name, payload, "administrator")


def test_lifecycle_events_stream_and_causal_order(client):
    create_template(client)
    task = client.post("/api/compute/tasks", json=submit_payload("evt-000001")).json()
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claimed.json()["task"]["id"] == task["id"]
    heartbeat = client.post(f"/api/compute/tasks/{task['id']}/heartbeat", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 120})
    assert heartbeat.status_code == 200
    completed = client.post(f"/api/compute/tasks/{task['id']}/complete", json={"worker_id": "w1", "result": {"value": 1}, "metrics": {"seconds": 2}})
    assert completed.status_code == 200
    events = client.get(f"/api/compute/events?task_id={task['id']}").json()["items"]
    assert [item["event_type"] for item in events] == ["task_submitted", "task_claimed", "lease_renewed", "task_completed"]
    assert [item["event_id"] for item in events] == sorted(item["event_id"] for item in events)
    assert [item["task_version"] for item in events] == [1, 2, 3, 4]
    assert events[1]["payload"]["worker_id"] == "w1"
    assert events[1]["payload"]["lease_seconds"] == 60
    assert events[3]["payload"]["result_version"] == 1
    assert all(item["project_code"] == "project-a" for item in events)


def test_failed_business_change_emits_no_event(client):
    create_template(client)
    task = client.post("/api/compute/tasks", json=submit_payload("evt-rollback")).json()
    rejected = client.post(f"/api/compute/tasks/{task['id']}/complete", json={"worker_id": "ghost", "result": {}, "metrics": {}})
    assert rejected.status_code == 409
    events = client.get(f"/api/compute/events?task_id={task['id']}").json()["items"]
    assert [item["event_type"] for item in events] == ["task_submitted"]


def test_idempotent_submission_emits_single_stable_event(client):
    create_template(client)
    for _ in range(3):
        response = client.post("/api/compute/tasks", json=submit_payload("evt-stable"))
        assert response.status_code == 202
    events = client.get("/api/compute/events?event_type=task_submitted").json()["items"]
    assert len(events) == 1
    assert events[0]["event_uid"] == f"task:{events[0]['task_id']}:task_submitted:1"


def test_subscription_filters_batch_and_resume(client):
    create_template(client)
    first = client.post("/api/compute/tasks", json=submit_payload("evt-filter-1")).json()
    second = client.post("/api/compute/tasks", json=submit_payload("evt-filter-2")).json()
    third = client.post("/api/compute/tasks", json=submit_payload("evt-filter-3")).json()
    client.post("/api/compute/tasks", json=submit_payload("evt-filter-4", project="project-b"))
    upserted = client.put(
        "/api/compute/event-subscriptions/billing-a?actor=administrator",
        json={"project_code": "project-a", "event_types": ["task_submitted"], "max_batch_size": 2},
    )
    assert upserted.status_code == 200, upserted.text
    batch = client.post("/api/compute/event-subscriptions/billing-a/claim", json={"consumer": "shift-1"}).json()
    assert [item["task_id"] for item in batch["items"]] == [first["id"], second["id"]]
    assert all(item["attempt"] == 1 for item in batch["items"])
    blocked = client.post("/api/compute/event-subscriptions/billing-a/claim", json={}).json()
    assert blocked["items"] == []
    ack = client.post("/api/compute/event-subscriptions/billing-a/ack", json={"event_ids": [item["event_id"] for item in batch["items"]]}).json()
    assert ack["acked"] == [item["event_id"] for item in batch["items"]]
    resumed = client.post("/api/compute/event-subscriptions/billing-a/claim", json={"consumer": "shift-2"}).json()
    assert [item["task_id"] for item in resumed["items"]] == [third["id"]]
    assert resumed["cursor_event_id"] < resumed["items"][0]["event_id"]


def test_subscription_validation_and_not_found(client):
    invalid = client.put("/api/compute/event-subscriptions/bad?actor=administrator", json={"event_types": ["unknown_type"]})
    assert invalid.status_code == 422
    missing = client.post("/api/compute/event-subscriptions/missing/claim", json={})
    assert missing.status_code == 404


def test_dead_letter_admin_api(client):
    create_template(client)
    client.post("/api/compute/tasks", json=submit_payload("evt-admin"))
    created = client.put("/api/compute/event-subscriptions/ops?actor=administrator", json={"lease_seconds": 5, "max_attempts": 1})
    assert created.status_code == 200
    claimed = client.post("/api/compute/event-subscriptions/ops/claim", json={}).json()
    event_id = claimed["items"][0]["event_id"]
    nack = client.post("/api/compute/event-subscriptions/ops/nack", json={"event_ids": [event_id], "reason": "格式错误"})
    assert nack.json()["dead"] == [event_id]
    letters = client.get("/api/compute/event-subscriptions/ops/dead-letters").json()["items"]
    assert [item["event_id"] for item in letters] == [event_id]
    assert letters[0]["dead_reason"] == "格式错误"
    requeue = client.post("/api/compute/event-subscriptions/ops/dead-letters/requeue?actor=administrator", json={})
    assert requeue.json()["requeued"] == [event_id]
    replay = client.post("/api/compute/event-subscriptions/ops/claim", json={}).json()
    assert [item["event_id"] for item in replay["items"]] == [event_id]
    detail = client.get("/api/compute/event-subscriptions/ops").json()
    assert detail["stats"]["requeued_events"] == 1
    listing = client.get("/api/compute/event-subscriptions").json()["items"]
    assert any(item["name"] == "ops" for item in listing)


def test_timeout_redelivery_dead_letter_and_requeue(client):
    clock, operations, events = make_services()
    operations.create_template(TEMPLATE, "administrator")
    task = operations.submit(submit_payload("evt-redelivery"))
    subscribe(events, "handover", lease_seconds=30, max_attempts=2)
    first = events.claim("handover", consumer="shift-a")
    assert [item["attempt"] for item in first["items"]] == [1]
    event_id = first["items"][0]["event_id"]
    clock.advance(seconds=31)
    second = events.claim("handover", consumer="shift-b")
    assert [item["event_id"] for item in second["items"]] == [event_id]
    assert second["items"][0]["attempt"] == 2
    clock.advance(seconds=31)
    third = events.claim("handover")
    assert third["items"] == []
    letters = events.dead_letters("handover")["items"]
    assert [item["event_id"] for item in letters] == [event_id]
    assert letters[0]["dead_reason"] == "attempts_exhausted"
    assert letters[0]["attempts"] == 2
    assert letters[0]["failure_count"] == 2
    follow_up = operations.submit(submit_payload("evt-follow-up"))
    fourth = events.claim("handover")
    assert [item["task_id"] for item in fourth["items"]] == [follow_up["id"]]
    before = events.list_events(task_id=task["id"])
    requeue = events.requeue_dead_letters("handover", None, "administrator")
    assert requeue["requeued"] == [event_id]
    replay = events.claim("handover")
    assert [item["event_id"] for item in replay["items"]] == [event_id]
    assert replay["items"][0]["attempt"] == 1
    assert events.list_events(task_id=task["id"]) == before
    ack = events.ack("handover", [event_id])
    assert ack["acked"] == [event_id]


def test_nack_releases_then_dead_letter_with_reason(client):
    _, operations, events = make_services()
    operations.create_template(TEMPLATE, "administrator")
    operations.submit(submit_payload("evt-nack"))
    subscribe(events, "billing", lease_seconds=300, max_attempts=2)
    first = events.claim("billing")
    event_id = first["items"][0]["event_id"]
    nack = events.nack("billing", [event_id], "计费规则缺失")
    assert nack["released"] == [event_id]
    second = events.claim("billing")
    assert [item["event_id"] for item in second["items"]] == [event_id]
    assert second["items"][0]["attempt"] == 2
    final = events.nack("billing", [event_id], "计费规则缺失")
    assert final["dead"] == [event_id]
    letters = events.dead_letters("billing")["items"]
    assert letters[0]["dead_reason"] == "计费规则缺失"
    assert letters[0]["failure_count"] == 2


def test_intervention_and_recovery_events(client):
    clock, operations, events = make_services()
    operations.create_template(TEMPLATE, "administrator")
    task = operations.submit(submit_payload("evt-intervene"))
    operations.cancel(task["id"], "administrator", "项目暂停")
    operations.retry(task["id"], "administrator", "项目恢复", priority=90)
    claimed = operations.claim("worker-a", ["solver-a"], 10)
    assert claimed and claimed["id"] == task["id"]
    clock.advance(seconds=11)
    recovered = operations.recover_expired()
    assert recovered["recovered"] == [task["id"]]
    stream = events.list_events(task_id=task["id"])
    assert [item["event_type"] for item in stream] == ["task_submitted", "manual_intervention", "manual_intervention", "task_claimed", "lease_recovered"]
    cancel_event = stream[1]
    assert cancel_event["payload"]["action"] == "cancel"
    assert cancel_event["payload"]["actor"] == "administrator"
    assert cancel_event["payload"]["before_status"] == "queued"
    assert cancel_event["payload"]["after_status"] == "cancelled"
    retry_event = stream[2]
    assert retry_event["payload"]["action"] == "retry"
    assert retry_event["payload"]["after_priority"] == 90
    recovery = stream[4]
    assert recovery["payload"]["previous_lease_owner"] == "worker-a"
    assert recovery["payload"]["outcome_status"] == "queued"


def test_subscription_stats_track_lag_failures_and_dead(client):
    clock, operations, events = make_services()
    operations.create_template(TEMPLATE, "administrator")
    for index in range(3):
        operations.submit(submit_payload(f"evt-stats-{index}"))
    subscribe(events, "handover", lease_seconds=10, max_attempts=1)
    stats = events.get_subscription("handover")["stats"]
    assert stats["unprocessed_events"] == 3
    assert stats["latest_event_id"] == 3
    first = events.claim("handover", limit=1)
    assert len(first["items"]) == 1
    stats = events.get_subscription("handover")["stats"]
    assert stats["in_flight_events"] == 1
    assert stats["awaiting_delivery_events"] == 2
    clock.advance(seconds=11)
    stats = events.get_subscription("handover")["stats"]
    assert stats["dead_events"] == 1
    assert stats["failures"] == 1
    assert stats["unprocessed_events"] == 2
    assert stats["lag_seconds"] >= 11
    rest = events.claim("handover")
    assert len(rest["items"]) == 2
    events.ack("handover", [item["event_id"] for item in rest["items"]])
    stats = events.get_subscription("handover")["stats"]
    assert stats["unprocessed_events"] == 0
    assert stats["acked_events"] == 2
    assert stats["cursor_event_id"] == stats["latest_event_id"]


def test_batches_preserve_global_and_causal_order_under_backlog(client):
    _, operations, events = make_services()
    operations.create_template(TEMPLATE, "administrator")
    for index in range(6):
        operations.submit(submit_payload(f"evt-order-{index}", priority=50 + index))
    claimed = operations.claim("worker-a", ["solver-a"], 60)
    assert claimed is not None
    operations.complete(claimed["id"], "worker-a", {"value": 1}, {})
    operations.cancel(2, "administrator", "演示取消")
    subscribe(events, "audit-trail", lease_seconds=30, max_attempts=3, max_batch_size=4)
    delivered: list[int] = []
    for _ in range(20):
        batch = events.claim("audit-trail")
        if not batch["items"]:
            break
        ids = [item["event_id"] for item in batch["items"]]
        assert ids == sorted(ids)
        delivered.extend(ids)
        events.ack("audit-trail", ids)
    assert delivered == sorted(delivered)
    assert len(delivered) == len(set(delivered))
    stats = events.get_subscription("audit-trail")["stats"]
    assert stats["unprocessed_events"] == 0
    versions: dict[int, list[int]] = {}
    for item in events.list_events(limit=500):
        versions.setdefault(item["task_id"], []).append(item["task_version"])
    for task_versions in versions.values():
        assert task_versions == sorted(task_versions)


def test_in_flight_batch_blocks_next_claim_until_ack_or_timeout(client):
    clock, operations, events = make_services()
    operations.create_template(TEMPLATE, "administrator")
    for index in range(3):
        operations.submit(submit_payload(f"evt-block-{index}"))
    subscribe(events, "handover", lease_seconds=30, max_attempts=3, max_batch_size=2)
    first = events.claim("handover")
    assert len(first["items"]) == 2
    blocked = events.claim("handover")
    assert blocked["items"] == []
    clock.advance(seconds=31)
    redelivered = events.claim("handover")
    assert [item["event_id"] for item in redelivered["items"]] == [item["event_id"] for item in first["items"]]
    assert [item["attempt"] for item in redelivered["items"]] == [2, 2]
    events.ack("handover", [item["event_id"] for item in redelivered["items"]])
    following = events.claim("handover")
    assert len(following["items"]) == 1
    assert following["items"][0]["event_id"] > redelivered["items"][-1]["event_id"]
