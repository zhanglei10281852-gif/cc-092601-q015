from __future__ import annotations

from fastapi import APIRouter, Path, Query

from app.compute.event_service import EventStreamService
from app.compute.schemas import DeadLetterRequeue, EventAck, EventClaim, EventNack, SubscriptionUpsert

router = APIRouter(prefix="/api/compute/events", tags=["计算运营事件流"])

CODE_PATTERN = r"^[a-z0-9][a-z0-9._-]+$"


def service() -> EventStreamService:
    return EventStreamService()


@router.get("/stream")
def stream(
    project_code: str | None = None,
    event_type: str | None = None,
    task_id: int | None = None,
    after_id: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
):
    return {"items": service().list_events(project_code=project_code, event_type=event_type, task_id=task_id, after_id=after_id, limit=limit)}


@router.put("/subscriptions/{code}")
def upsert_subscription(payload: SubscriptionUpsert, code: str = Path(min_length=2, max_length=64, pattern=CODE_PATTERN)):
    return service().upsert_subscription(code, payload.model_dump())


@router.get("/subscriptions")
def list_subscriptions():
    return {"items": service().list_subscriptions()}


@router.get("/subscriptions/{code}")
def get_subscription(code: str):
    return service().get_subscription(code)


@router.post("/subscriptions/{code}/claim")
def claim_events(payload: EventClaim, code: str):
    return service().claim(code, payload.consumer_id, payload.batch_size, payload.lease_seconds)


@router.post("/subscriptions/{code}/ack")
def ack_events(payload: EventAck, code: str):
    return service().ack(code, payload.consumer_id, payload.event_ids)


@router.post("/subscriptions/{code}/nack")
def nack_events(payload: EventNack, code: str):
    return service().nack(code, payload.consumer_id, payload.event_ids, payload.error)


@router.get("/subscriptions/{code}/dead-letters")
def dead_letters(code: str):
    return {"items": service().dead_letters(code)}


@router.post("/subscriptions/{code}/dead-letters/requeue")
def requeue_dead_letters(payload: DeadLetterRequeue, code: str):
    return service().requeue_dead(code, payload.event_ids)
