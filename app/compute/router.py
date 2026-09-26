from __future__ import annotations

from fastapi import APIRouter, Path, Query

from app.compute.events import ComputeEventService
from app.compute.schemas import BatchOperation, CancelRequest, DeadLetterRequeueRequest, EventAckRequest, EventClaimRequest, EventNackRequest, EventSubscriptionUpsert, PriorityRequest, QuotaSet, RetryRequest, TaskClaim, TaskFailure, TaskResult, TaskSubmit, TemplateCreate
from app.compute.service import ComputeOperationsService

router = APIRouter(prefix="/api/compute", tags=["科学计算任务运营"])


def service() -> ComputeOperationsService:
    return ComputeOperationsService()


def event_service() -> ComputeEventService:
    return ComputeEventService()


@router.get("/templates")
def list_templates():
    return {"items": service().list_templates()}


@router.post("/templates", status_code=201)
def create_template(payload: TemplateCreate, actor: str = Query(..., min_length=1)):
    return service().create_template(payload.model_dump(), actor)


@router.put("/quotas")
def set_quota(payload: QuotaSet, actor: str = Query(..., min_length=1)):
    return service().set_quota(payload.model_dump(), actor)


@router.post("/tasks", status_code=202)
def submit_task(payload: TaskSubmit):
    return service().submit(payload.model_dump())


@router.get("/tasks")
def list_tasks(status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = Query(default=100, ge=1, le=500)):
    return {"items": service().list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=limit)}


@router.get("/task-details/{task_id}")
def get_task(task_id: int):
    return service().get_task(task_id)


@router.post("/tasks/claim")
def claim_task(payload: TaskClaim):
    return {"task": service().claim(payload.worker_id, payload.capabilities, payload.lease_seconds)}


@router.post("/tasks/{task_id}/heartbeat")
def heartbeat(task_id: int, payload: TaskClaim):
    return service().heartbeat(task_id, payload.worker_id, payload.lease_seconds)


@router.post("/tasks/{task_id}/complete")
def complete_task(task_id: int, payload: TaskResult):
    return service().complete(task_id, payload.worker_id, payload.result, payload.metrics)


@router.post("/tasks/{task_id}/fail")
def fail_task(task_id: int, payload: TaskFailure):
    return service().fail(task_id, payload.worker_id, payload.error_code, payload.message, payload.retryable)


@router.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: int, payload: CancelRequest):
    return service().cancel(task_id, payload.actor, payload.reason)


@router.post("/tasks/{task_id}/retry")
def retry_task(task_id: int, payload: RetryRequest):
    return service().retry(task_id, payload.actor, payload.reason, payload.priority)


@router.post("/tasks/{task_id}/priority")
def set_priority(task_id: int, payload: PriorityRequest):
    return service().set_priority(task_id, payload.actor, payload.reason, payload.priority)


@router.post("/tasks/batch")
def batch_operation(payload: BatchOperation):
    return service().batch_operation(payload.model_dump())


@router.post("/recovery/expired-leases")
def recover_expired(actor: str = Query(default="recovery-worker", min_length=1)):
    return service().recover_expired(actor)


@router.get("/summary")
def summary():
    return service().summary()


@router.put("/event-subscriptions/{name}", status_code=200)
def upsert_event_subscription(payload: EventSubscriptionUpsert, name: str = Path(pattern=r"^[a-z0-9][a-z0-9._-]{1,63}$"), actor: str = Query(..., min_length=1)):
    return event_service().upsert_subscription(name, payload.model_dump(), actor)


@router.get("/event-subscriptions")
def list_event_subscriptions():
    return {"items": event_service().list_subscriptions()}


@router.get("/event-subscriptions/{name}")
def get_event_subscription(name: str):
    return event_service().get_subscription(name)


@router.post("/event-subscriptions/{name}/claim")
def claim_events(payload: EventClaimRequest, name: str):
    return event_service().claim(name, limit=payload.limit, consumer=payload.consumer)


@router.post("/event-subscriptions/{name}/ack")
def ack_events(payload: EventAckRequest, name: str):
    return event_service().ack(name, payload.event_ids)


@router.post("/event-subscriptions/{name}/nack")
def nack_events(payload: EventNackRequest, name: str):
    return event_service().nack(name, payload.event_ids, payload.reason)


@router.get("/event-subscriptions/{name}/dead-letters")
def list_dead_letters(name: str, limit: int = Query(default=100, ge=1, le=500)):
    return event_service().dead_letters(name, limit)


@router.post("/event-subscriptions/{name}/dead-letters/requeue")
def requeue_dead_letters(payload: DeadLetterRequeueRequest, name: str, actor: str = Query(..., min_length=1)):
    return event_service().requeue_dead_letters(name, payload.event_ids, actor)


@router.get("/events")
def list_events(project_code: str | None = None, event_type: str | None = None, task_id: int | None = None, since_id: int = Query(default=0, ge=0), limit: int = Query(default=100, ge=1, le=500)):
    return {"items": event_service().list_events(project_code=project_code, event_type=event_type, task_id=task_id, since_id=since_id, limit=limit)}
