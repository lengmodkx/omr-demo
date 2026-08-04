"""模板相关路由:
- POST /v1/templates/parse - 同步模板解析
- POST /v1/verify_recognition_rate - 暂返 501
- POST /v1/reverify_paper - 与 recognize 等价
"""
from __future__ import annotations

from fastapi import APIRouter, Request

from omr_service.api.deps import get_service
from omr_service.api.schemas.recognize import RecognizeRequest, RecognizeResponse
from omr_service.api.schemas.templates import GoldenTemplateRequest, GoldenTemplateResponse

router = APIRouter(prefix="/v1", tags=["templates"])


@router.post("/templates/parse", response_model=GoldenTemplateResponse)
def parse_template(request: Request, body: GoldenTemplateRequest):
    service = get_service(request)
    return service.parse_golden_template(body.model_dump())


@router.post("/reverify_paper", response_model=RecognizeResponse)
def reverify_paper(request: Request, body: RecognizeRequest):
    service = get_service(request)
    return service.reverify_paper(body.model_dump())


@router.post("/verify_recognition_rate")
def verify_recognition_rate(request: Request):
    service = get_service(request)
    return service.verify_recognition_rate({})
