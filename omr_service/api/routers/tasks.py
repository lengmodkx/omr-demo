"""异步任务 REST 包装.

- POST /v1/tasks - 投递异步任务 (XADD omr:batch:job + 写 Hash queued)
- GET /v1/tasks/{task_id} - 查询任务状态 (读 Hash)

底层调用 omr_service.mq.producer.enqueue_job.
"""
from __future__ import annotations

from datetime import datetime, timezone
import uuid

from fastapi import APIRouter, Request

from omr_service.api.deps import get_task_registry
from omr_service.api.schemas.enums import TaskStatus
from omr_service.api.schemas.tasks import (
    CreateTaskRequest,
    TaskCreatedResponse,
    TaskStatusResponse,
)

# 复用现有 mq.producer
from omr_service.mq.producer import enqueue_job  # noqa: E402

router = APIRouter(prefix="/v1/tasks", tags=["tasks"])


def _validate_payload(task_type: str, payload: dict) -> None:
    """根据 task_type 二次校验 payload."""
    if task_type == "recognize":
        if not payload.get("template_id") or not payload.get("scan_image_urls"):
            from omr_service.core.exceptions import InvalidRequestError
            raise InvalidRequestError("payload", "template_id or scan_image_urls missing")
    elif task_type == "parse_template":
        if not payload.get("template_id") or not payload.get("template_image_url"):
            from omr_service.core.exceptions import InvalidRequestError
            raise InvalidRequestError("payload", "template_id or template_image_url missing")
    else:
        from omr_service.core.exceptions import InvalidRequestError
        raise InvalidRequestError("task_type", f"unknown: {task_type}")


@router.post("", status_code=202, response_model=TaskCreatedResponse)
def create_task(request: Request, body: CreateTaskRequest):
    """投递异步任务."""
    task_registry = get_task_registry(request)
    task_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()

    _validate_payload(body.task_type.value, body.payload)

    # 复用现有 mq.producer
    enqueue_job(
        task_type=body.task_type.value,
        payload=body.payload,
        task_id=task_id,
    )

    # 写 Hash 标记 queued
    task_registry.write_queued(
        task_id=task_id,
        task_type=body.task_type.value,
        payload=body.payload,
        created_at=created_at,
    )

    return TaskCreatedResponse(
        task_id=task_id,
        status=TaskStatus.QUEUED,
        created_at=created_at,
    )


@router.get("/{task_id}", response_model=TaskStatusResponse)
def get_task(request: Request, task_id: str):
    """查询任务状态."""
    task_registry = get_task_registry(request)
    task = task_registry.get(task_id)
    return TaskStatusResponse(**task)