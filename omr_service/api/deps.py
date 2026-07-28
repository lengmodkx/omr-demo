"""FastAPI 依赖注入."""
from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import Request

if TYPE_CHECKING:
    from fastapi import FastAPI

    from omr_service.config import OmrSettings
    from omr_service.core.service import OmrService
    from omr_service.core.task_registry import TaskRegistry


def get_settings(request: Request) -> "OmrSettings":
    return request.app.state.settings


def get_service(request: Request) -> "OmrService":
    return request.app.state.service


def get_task_registry(request: Request) -> "TaskRegistry":
    return request.app.state.task_registry


def register_dependencies(
    app: "FastAPI",
    *,
    settings: "OmrSettings",
    service: "OmrService",
    task_registry: "TaskRegistry",
) -> None:
    """把核心组件挂到 app.state, 供 Depends 调用."""
    app.state.settings = settings
    app.state.service = service
    app.state.task_registry = task_registry
