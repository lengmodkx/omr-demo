"""健康检查路由.

- GET /v1/health - 存活
- GET /v1/health/ready - 就绪 (依赖服务检查)
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from omr_service.api.deps import get_service, get_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["health"])


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/health/ready")
def ready(request: Request):
    """检查 TemplateStore / Redis / service 是否就绪."""
    settings = get_settings(request)
    service = get_service(request)
    checks = {"service": True, "template_store": service.template_store is not None}

    # Redis 就绪检查
    if getattr(settings, "redis_enabled", False):
        try:
            from omr_service.mq.client import get_redis_client
            r = get_redis_client()
            r.ping()
            checks["redis"] = True
        except Exception as e:
            logger.warning("Redis health check failed: %s", e)
            checks["redis"] = False
    else:
        checks["redis"] = True  # Redis 未启用时不检查

    healthy = all(checks.values())
    status_code = 200 if healthy else 503
    return JSONResponse(
        status_code=status_code,
        content={"status": "ok" if healthy else "degraded", "checks": checks},
    )