"""POST /v1/recognize - 同步识别."""
from __future__ import annotations

from fastapi import APIRouter, Request

from omr_service.api.deps import get_service
from omr_service.api.schemas.recognize import RecognizeRequest, RecognizeResponse

router = APIRouter(prefix="/v1", tags=["recognize"])


@router.post("/recognize", response_model=RecognizeResponse)
def recognize(
    request: Request,
    body: RecognizeRequest,
):
    """同步识别答题卡. 同步返回完整结果 (5-30s)."""
    service = get_service(request)
    result = service.recognize(body.model_dump())
    return result
