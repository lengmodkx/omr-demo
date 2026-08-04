"""GET /v1/omr_crops/{file_path:path} - 静态裁剪图服务.

安全: 使用 resolve() 防止路径穿越.
"""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse

from omr_service.api.deps import get_settings

router = APIRouter(prefix="/v1/omr_crops", tags=["crops"])


@router.get("/{file_path:path}")
def get_crop(request: Request, file_path: str):
    settings = get_settings(request)
    base = Path(settings.crop_output_dir).resolve()
    target = (base / file_path).resolve()

    # 路径穿越检查
    try:
        target.relative_to(base)
    except ValueError:
        raise HTTPException(status_code=400, detail="path traversal detected")

    if not target.is_file():
        raise HTTPException(status_code=404, detail="file not found")

    return FileResponse(target)
