"""统一异常处理: OmrError → HTTP 响应.

错误码与 HTTP status 映射:
    4 → 404 (TemplateNotFoundError)
    5 → 502 (ImageLoadError)
    6 → 400 (InvalidRequestError)
    7 → 404 (TaskNotFoundError)
    99 → 500 (InternalError / 未捕获异常)
"""
from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from omr_service.core.exceptions import OmrError

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger(__name__)


_STATUS_MAP: dict[int, int] = {
    4: 404,
    5: 502,
    6: 400,
    7: 404,
    99: 500,
}


def _request_id(request: Request) -> str:
    return getattr(request.state, "request_id", None) or str(uuid.uuid4())


async def _omr_error_handler(request: Request, exc: OmrError) -> JSONResponse:
    request_id = _request_id(request)
    status_code = _STATUS_MAP.get(exc.code, 500)
    logger.warning(
        "omr_error: code=%s message=%s request_id=%s",
        exc.code, exc.message, request_id,
    )
    return JSONResponse(
        status_code=status_code,
        content={"code": exc.code, "message": exc.message, "request_id": request_id},
    )


async def _validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    request_id = _request_id(request)
    logger.warning("validation_error: %s request_id=%s", exc.errors(), request_id)
    first_error = exc.errors()[0] if exc.errors() else {}
    field = ".".join(str(p) for p in first_error.get("loc", []))
    msg = first_error.get("msg", "请求参数非法")
    return JSONResponse(
        status_code=400,
        content={"code": 6, "message": f"请求参数非法: {field} ({msg})", "request_id": request_id},
    )


async def _unhandled_handler(request: Request, exc: Exception) -> JSONResponse:
    request_id = _request_id(request)
    logger.exception("unhandled_exception: request_id=%s", request_id, exc_info=exc)
    return JSONResponse(
        status_code=500,
        content={"code": 99, "message": "内部错误", "request_id": request_id},
    )


def register_error_handlers(app: "FastAPI") -> None:
    """全局注册异常 handler."""
    app.add_exception_handler(OmrError, _omr_error_handler)
    app.add_exception_handler(RequestValidationError, _validation_error_handler)
    app.add_exception_handler(Exception, _unhandled_handler)