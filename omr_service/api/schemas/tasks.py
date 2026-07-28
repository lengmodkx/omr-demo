from datetime import datetime
from pydantic import BaseModel, Field

from omr_service.api.schemas.enums import TaskStatus, TaskType


class CreateTaskRequest(BaseModel):
    task_type: TaskType
    # payload 用 dict, 由 router 根据 task_type 二次校验
    payload: dict


class TaskCreatedResponse(BaseModel):
    task_id: str
    status: TaskStatus = TaskStatus.QUEUED
    created_at: datetime


class TaskStatusResponse(BaseModel):
    task_id: str
    task_type: TaskType
    status: TaskStatus
    created_at: datetime
    finished_at: datetime | None = None
    result: dict | None = None
    error: dict | None = None
