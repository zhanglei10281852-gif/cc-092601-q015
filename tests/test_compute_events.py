from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.compute.event_service import EventStreamService
from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.errors import ConflictError


TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}


def submit_payload(key: str, *, user: str = "researcher-1", project: str = "project-a") -> dict:
    return {
        "template_code": "solver-a",
        "project_code": project,
        "requested_by": user,
        "parameters": {"iterations": 100, "mode": "accurate"},
        "priority": 50,
        "idempotency_key": key,
    }


def sub_payload(**overrides) -> dict:
    payload = {
        "description": "",
        "event_types": [],
        "project_codes": [],
        "lease_seconds": 30,
        "max_delivery_attempts": 5,
        "start_from": "beginning",
        "reset_cursor": False,
    }
    payload.update(overrides)
    return payload


@pytest.fixture()
def db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TOWNSHIP_DATABASE_PATH", str(tmp_path / "events.db"))
    from app.database import close_connection, init_db

    close_connection()
    init_db()
    yield
    close_connection()


def create_template(client) -> None:
    response = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert response.status_code == 201, response.text


def test_business_changes_emit_immutable_events_in_order(client):
    create_template(client)
    task = client.post("/api/compute/tasks", json=submit_payload("evt-000001")).json()
    replay = client.post("/api/compute/tasks", json=submit_payload("evt-000001"))
    assert replay.status_code == 202 and replay.json()["id"] == task["id"]
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60}).json()["task"]
    client.post(f"/api/compute/tasks/{task['id']}/heartbeat", json={"worker_id": "w1", "capabilities": [], "lease_seconds": 120})
    client.post(f"/api/compute/tasks/{task['id']}/complete", json={"worker_id": "w1", "result": {"value": 1}, "metrics": {}})

    events = client.get("/api/compute/events/stream").json()["items"]
    assert [event["event_type"] for event in events] == ["task_submitted", "task_claimed", "lease_renewed", "task_completed"]
    keys = [event["event_key"] for event in events]
    assert len(set(keys)) == len(keys)
    assert keys[0] == f"task_submitted:{task['id']}:1"
    assert [event["task_version"] for event in events] == [1, 2, 3, 4]
    # 载荷是变更后的快照，事件本身不随后续操作改变
    assert events[0]["payload"]["task"]["status"] == "queued"
    assert events[1]["payload"]["task"]["status"] == "running"
    assert events[1]["payload"]["detail"] == {"worker_id": "w1", "lease_expires_at": claimed["lease_expires_at"], "attempt": 1}
    assert events[3]["payload"]["detail"]["result_version"] == 1
    # 幂等重放没有产生第二笔提交事件
    assert [event["event_type"] for event in events].count("task_submitted") == 1


def test_intervention_and_recovery_events(client):
    create_template(client)
    task = client.post("/api/compute/tasks", json=submit_payload("evt-int-1")).json()
    client.post(f"/api/compute/tasks/{task['id']}/cancel", json={"actor": "administrator", "reason": "项目暂停"})
    client.post(f"/api/compute/tasks/{task['id']}/retry", json={"actor": "administrator", "reason": "项目恢复", "priority": 90})
    client.post(f"/api/compute/tasks/{task['id']}/priority", json={"actor": "administrator", "reason": "加急处理", "priority": 99})
    events = client.get("/api/compute/events/stream?event_type=task_intervened").json()["items"]
    assert [event["payload"]["detail"]["action"] for event in events] == ["cancel", "retry", "priority"]
    assert all(event["payload"]["detail"]["actor"] == "administrator" for event in events)
    by_task = client.get(f"/api/compute/events/stream?task_id={task['id']}").json()["items"]
    assert [event["event_type"] for event in by_task] == ["task_submitted", "task_intervened", "task_intervened", "task_intervened"]


def test_recovery_emits_task_recovered_events(db):
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    compute = ComputeOperationsService(clock=clock)
    compute.create_template(TEMPLATE, "administrator")
    task = compute.submit(submit_payload("evt-rec-1"))
    compute.claim("worker-a", ["solver-a"], 10)
    clock.advance(seconds=11)
    recovered = compute.recover_expired()
    assert recovered["recovered"] == [task["id"]]
    events = EventStreamService(clock=clock).list_events(event_type="task_recovered")
    assert len(events) == 1
    assert events[0]["payload"]["detail"] == {"actor": "recovery-worker", "outcome": "requeued", "previous_lease_owner": "worker-a"}


def test_stream_filters_and_type_validation(client):
    create_template(client)
    client.post("/api/compute/tasks", json=submit_payload("evt-q-1"))
    client.post("/api/compute/tasks", json=submit_payload("evt-q-2", project="project-b"))
    by_project = client.get("/api/compute/events/stream?project_code=project-b").json()["items"]
    assert len(by_project) == 1 and by_project[0]["project_code"] == "project-b"
    by_type = client.get("/api/compute/events/stream?event_type=task_submitted").json()["items"]
    assert len(by_type) == 2
    assert client.get("/api/compute/events/stream?event_type=nope").status_code == 422
    assert client.put("/api/compute/events/subscriptions/bad-sub", json={"event_types": ["nope"]}).status_code == 422


def test_subscription_filtering_batch_claim_and_resume(client):
    create_template(client)
    client.post("/api/compute/tasks", json=submit_payload("flt-a-1"))
    client.post("/api/compute/tasks", json=submit_payload("flt-b-1", project="project-b"))
    client.post("/api/compute/tasks", json=submit_payload("flt-a-2"))
    created = client.put(
        "/api/compute/events/subscriptions/duty-handover",
        json={"description": "值班交接", "project_codes": ["project-a"], "lease_seconds": 30},
    )
    assert created.status_code == 200, created.text
    assert created.json()["cursor"]["pending_events"] == 2

    claimed = client.post("/api/compute/events/subscriptions/duty-handover/claim", json={"consumer_id": "c1", "batch_size": 10}).json()
    ids = [item["event_id"] for item in claimed["deliveries"]]
    assert ids == sorted(ids) and len(ids) == 2
    assert all(item["event"]["project_code"] == "project-a" for item in claimed["deliveries"])
    assert all(item["attempt"] == 1 for item in claimed["deliveries"])

    acked = client.post("/api/compute/events/subscriptions/duty-handover/ack", json={"consumer_id": "c1", "event_ids": ids}).json()
    assert acked["last_ack_event_id"] == ids[-1]
    # 重启后（新的消费者实例）从最后确认位置继续，已确认事件不再投递
    again = client.post("/api/compute/events/subscriptions/duty-handover/claim", json={"consumer_id": "c2", "batch_size": 10}).json()
    assert again["deliveries"] == []
    client.post("/api/compute/tasks", json=submit_payload("flt-a-3"))
    follow = client.post("/api/compute/events/subscriptions/duty-handover/claim", json={"consumer_id": "c2", "batch_size": 10}).json()
    assert [item["event"]["event_type"] for item in follow["deliveries"]] == ["task_submitted"]
    assert follow["deliveries"][0]["event_id"] > ids[-1]


def test_event_type_filtering_for_billing(client):
    create_template(client)
    task = client.post("/api/compute/tasks", json=submit_payload("flt-type-1")).json()
    client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    client.put("/api/compute/events/subscriptions/internal-billing", json={"event_types": ["task_submitted", "task_completed"]})
    claimed = client.post("/api/compute/events/subscriptions/internal-billing/claim", json={"consumer_id": "billing-1", "batch_size": 10}).json()
    assert [item["event"]["event_type"] for item in claimed["deliveries"]] == ["task_submitted"]
    client.post(f"/api/compute/tasks/{task['id']}/complete", json={"worker_id": "w1", "result": {}, "metrics": {}})
    claimed = client.post("/api/compute/events/subscriptions/internal-billing/claim", json={"consumer_id": "billing-1", "batch_size": 10}).json()
    assert [item["event"]["event_type"] for item in claimed["deliveries"]] == ["task_completed"]


def test_ack_requires_lease_holder_and_known_subscription(client):
    create_template(client)
    client.post("/api/compute/tasks", json=submit_payload("hold-000001"))
    client.put("/api/compute/events/subscriptions/duty", json={})
    claimed = client.post("/api/compute/events/subscriptions/duty/claim", json={"consumer_id": "c1", "batch_size": 1}).json()
    event_id = claimed["deliveries"][0]["event_id"]
    wrong = client.post("/api/compute/events/subscriptions/duty/ack", json={"consumer_id": "c2", "event_ids": [event_id]})
    assert wrong.status_code == 409
    missing = client.post("/api/compute/events/subscriptions/unknown-sub/claim", json={"consumer_id": "c1", "batch_size": 1})
    assert missing.status_code == 404


def test_timeout_redelivery_counts_attempts(db):
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    compute = ComputeOperationsService(clock=clock)
    events = EventStreamService(clock=clock)
    compute.create_template(TEMPLATE, "administrator")
    compute.submit(submit_payload("timeout-0001"))
    events.upsert_subscription("billing", sub_payload(lease_seconds=5))

    first = events.claim("billing", "consumer-1", 10)
    assert [item["attempt"] for item in first["deliveries"]] == [1]
    event_id = first["deliveries"][0]["event_id"]
    # 租约未过期时其他消费者领不到
    assert events.claim("billing", "consumer-2", 10)["deliveries"] == []
    clock.advance(seconds=6)
    again = events.claim("billing", "consumer-2", 10)
    assert [item["event_id"] for item in again["deliveries"]] == [event_id]
    assert again["deliveries"][0]["attempt"] == 2
    # 旧消费者已失去租约，确认被拒绝
    with pytest.raises(ConflictError):
        events.ack("billing", "consumer-1", [event_id])
    done = events.ack("billing", "consumer-2", [event_id])
    assert done["last_ack_event_id"] == event_id


def test_nack_dead_letter_and_requeue_keeps_event(client):
    create_template(client)
    client.post("/api/compute/tasks", json=submit_payload("dlq-000001"))
    client.post("/api/compute/tasks", json=submit_payload("dlq-000002"))
    client.put("/api/compute/events/subscriptions/billing", json={"max_delivery_attempts": 1, "lease_seconds": 30})
    claimed = client.post("/api/compute/events/subscriptions/billing/claim", json={"consumer_id": "c1", "batch_size": 10}).json()
    ids = [item["event_id"] for item in claimed["deliveries"]]
    assert len(ids) == 2

    nacked = client.post("/api/compute/events/subscriptions/billing/nack", json={"consumer_id": "c1", "event_ids": [ids[0]], "error": "计费接口超时"}).json()
    assert nacked["results"] == [{"event_id": ids[0], "status": "dead"}]
    # 死信不会阻塞后续事件，游标越过死信继续前进
    acked = client.post("/api/compute/events/subscriptions/billing/ack", json={"consumer_id": "c1", "event_ids": [ids[1]]}).json()
    assert acked["last_ack_event_id"] == ids[1]

    dead = client.get("/api/compute/events/subscriptions/billing/dead-letters").json()["items"]
    assert len(dead) == 1
    assert dead[0]["event_id"] == ids[0]
    assert dead[0]["dead_reason"] == "计费接口超时"
    assert dead[0]["attempts"] == 1

    before = client.get(f"/api/compute/events/stream?after_id={ids[0] - 1}&limit=1").json()["items"][0]
    requeued = client.post("/api/compute/events/subscriptions/billing/dead-letters/requeue", json={}).json()
    assert requeued["requeued_event_ids"] == [ids[0]]
    reclaimed = client.post("/api/compute/events/subscriptions/billing/claim", json={"consumer_id": "c2", "batch_size": 10}).json()
    assert [item["event_id"] for item in reclaimed["deliveries"]] == [ids[0]]
    assert reclaimed["deliveries"][0]["attempt"] == 1
    assert reclaimed["deliveries"][0]["event"]["event_key"] == before["event_key"]
    after = client.get(f"/api/compute/events/stream?after_id={ids[0] - 1}&limit=1").json()["items"][0]
    assert after == before  # 重新投入不改写原事件
    assert client.get("/api/compute/events/subscriptions/billing/dead-letters").json()["items"] == []


def test_exhausted_lease_attempts_become_dead_letters(db):
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    compute = ComputeOperationsService(clock=clock)
    events = EventStreamService(clock=clock)
    compute.create_template(TEMPLATE, "administrator")
    compute.submit(submit_payload("poison-0001"))
    events.upsert_subscription("billing", sub_payload(lease_seconds=5, max_delivery_attempts=2))
    for expected_attempt in (1, 2):
        batch = events.claim("billing", "worker", 10)
        assert [item["attempt"] for item in batch["deliveries"]] == [expected_attempt]
        clock.advance(seconds=6)
    # 第三次领取时投递次数已耗尽，转为死信而不是继续重投
    assert events.claim("billing", "worker", 10)["deliveries"] == []
    dead = events.dead_letters("billing")
    assert len(dead) == 1 and dead[0]["attempts"] == 2
    assert "最大投递次数" in dead[0]["dead_reason"]


def test_admin_stats_track_lag_failures_and_dead(client):
    create_template(client)
    for index in range(3):
        client.post("/api/compute/tasks", json=submit_payload(f"stat-{index:06d}"))
    client.put("/api/compute/events/subscriptions/duty", json={"lease_seconds": 30})
    stats = client.get("/api/compute/events/subscriptions/duty").json()
    assert stats["cursor"]["pending_events"] == 3
    assert stats["cursor"]["latest_event_id"] == 3
    assert stats["cursor"]["last_ack_event_id"] == 0
    assert stats["cursor"]["lag_seconds"] >= 0

    claimed = client.post("/api/compute/events/subscriptions/duty/claim", json={"consumer_id": "c1", "batch_size": 2}).json()
    ids = [item["event_id"] for item in claimed["deliveries"]]
    client.post("/api/compute/events/subscriptions/duty/nack", json={"consumer_id": "c1", "event_ids": [ids[0]], "error": "临时故障"})
    client.post("/api/compute/events/subscriptions/duty/ack", json={"consumer_id": "c1", "event_ids": [ids[1]]})
    stats = client.get("/api/compute/events/subscriptions/duty").json()
    assert stats["deliveries"]["acked"] == 1
    assert stats["deliveries"]["failed"] == 1
    assert stats["deliveries"]["attempts"] == 2
    assert stats["cursor"]["pending_events"] == 2  # 未确认的失败投递 + 未领取的第三条
    listed = client.get("/api/compute/events/subscriptions").json()["items"]
    assert [item["code"] for item in listed] == ["duty"]


def test_global_and_causal_order_under_backlog(db):
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    compute = ComputeOperationsService(clock=clock)
    events = EventStreamService(clock=clock)
    compute.create_template(TEMPLATE, "administrator")
    for index in range(5):
        compute.submit(submit_payload(f"backlog-{index:06d}"))
        claimed = compute.claim("worker-a", ["solver-a"], 30)
        compute.heartbeat(claimed["id"], "worker-a", 30)
        compute.complete(claimed["id"], "worker-a", {"value": index}, {})
    events.upsert_subscription("audit-trail", sub_payload())

    delivered = []
    while True:
        batch = events.claim("audit-trail", "consumer", 6)
        if not batch["deliveries"]:
            break
        events.ack("audit-trail", "consumer", [item["event_id"] for item in batch["deliveries"]])
        delivered.extend(batch["deliveries"])
    assert len(delivered) == 20
    ids = [item["event_id"] for item in delivered]
    assert ids == sorted(ids)  # 全局顺序跨批次保持
    versions_by_task: dict[int, list[int]] = {}
    for item in delivered:
        event = item["event"]
        versions_by_task.setdefault(event["task_id"], []).append(event["task_version"])
    assert len(versions_by_task) == 5
    for versions in versions_by_task.values():
        assert versions == sorted(versions)  # 每个任务内因果顺序保持
    view = events.get_subscription("audit-trail")
    assert view["cursor"]["pending_events"] == 0
    assert view["cursor"]["last_ack_event_id"] == ids[-1]


def test_partial_ack_keeps_cursor_at_contiguous_position(db):
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    compute = ComputeOperationsService(clock=clock)
    events = EventStreamService(clock=clock)
    compute.create_template(TEMPLATE, "administrator")
    for index in range(3):
        compute.submit(submit_payload(f"partial-{index:06d}"))
    events.upsert_subscription("duty", sub_payload(lease_seconds=5))
    batch = events.claim("duty", "consumer", 10)
    ids = [item["event_id"] for item in batch["deliveries"]]
    assert len(ids) == 3
    # 只确认第一条，游标停在第一条；剩余两条租约过期后重新投递
    result = events.ack("duty", "consumer", ids[:1])
    assert result["last_ack_event_id"] == ids[0]
    clock.advance(seconds=6)
    redelivered = events.claim("duty", "consumer", 10)
    assert [item["event_id"] for item in redelivered["deliveries"]] == ids[1:]
