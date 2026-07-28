"""OMR 业务核心: 协议无关, 输入输出都是 plain dict.

本模块不依赖 FastAPI / Pydantic / protobuf.

现有实现参考 omr_service/rpc/omr_service.py, 本任务是抽离 + dict 接口.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from omr_service.core.exceptions import (
    ImageLoadError,
    InternalError,
    InvalidRequestError,
    TemplateNotFoundError,
)

logger = logging.getLogger(__name__)


class OmrService:
    """协议无关的 OMR 业务服务."""

    def __init__(
        self,
        template_store,
        image_loader,
        worker_pool,
        ocr_engine,
        cropper,
        *,
        sync_timeout_seconds: float = 60.0,
    ):
        self.template_store = template_store
        self.image_loader = image_loader
        self.worker_pool = worker_pool
        self.ocr_engine = ocr_engine
        self.cropper = cropper
        self.sync_timeout_seconds = sync_timeout_seconds

    def recognize(self, request: dict[str, Any]) -> dict[str, Any]:
        """同步识别. 返回 RecognizeResult dict."""
        template_id = request.get("template_id")
        scan_urls = request.get("scan_image_urls")
        if not template_id or not scan_urls:
            raise InvalidRequestError("template_id or scan_image_urls", "missing")

        t0 = time.monotonic()
        images = self._load_images(scan_urls)
        template = self.template_store.get(template_id)
        if template is None:
            raise TemplateNotFoundError(template_id)

        future = self.worker_pool.submit(self._do_recognize, template, images, request)
        try:
            answers, abnormal = future.result(timeout=self.sync_timeout_seconds)
        except TimeoutError as e:
            raise InternalError(f"识别超时 ({self.sync_timeout_seconds}s)") from e

        result = {
            "code": 0,
            "message": "ok",
            "template_id": template_id,
            "answers": answers,
            "abnormal": abnormal,
            "empty_count": sum(1 for a in answers if a.get("is_blank")),
            "multiple_count": sum(1 for a in answers if a.get("is_multiple")),
            "elapsed_ms": int((time.monotonic() - t0) * 1000),
        }

        # OCR 个人信息
        if request.get("personal_info_region"):
            result["personal_info"] = self.ocr_engine.recognize(images)

        # 主观题裁剪
        if request.get("subjective_regions"):
            result["subjective_crops"] = self.cropper.crop(images, request["subjective_regions"])

        return result

    def parse_golden_template(self, request: dict[str, Any]) -> dict[str, Any]:
        """同步模板解析. 返回 GoldenTemplateResult dict."""
        template_id = request.get("template_id")
        template_url = request.get("template_image_url")
        columns = request.get("columns", [])
        if not template_id or not template_url:
            raise InvalidRequestError("template_id or template_image_url", "missing")
        if not columns:
            raise InvalidRequestError("columns", "empty")

        t0 = time.monotonic()
        images = self._load_images([template_url])
        answers, bubble_grid = self._do_parse(images[0], columns)

        result = {
            "code": 0,
            "message": "ok",
            "template_id": template_id,
            "answers": answers,
            "bubble_grid": bubble_grid,
            "elapsed_ms": int((time.monotonic() - t0) * 1000),
        }

        # 黄金模板阶段示例 OCR（用于确认位置，不强制要求准确）
        if request.get("personal_info_region"):
            result["personal_info_sample"] = self.ocr_engine.recognize(images)

        return result

    def verify_recognition_rate(self, request: dict[str, Any]) -> dict[str, Any]:
        """暂未实现."""
        raise InternalError("verify_recognition_rate 暂未通过 HTTP 暴露")

    def reverify_paper(self, request: dict[str, Any]) -> dict[str, Any]:
        """与 recognize 行为等价."""
        return self.recognize(request)

    # ---------- 内部辅助 ----------

    def _load_images(self, urls: list[str]) -> list:
        try:
            return self.image_loader.load(urls)
        except FileNotFoundError as e:
            raise ImageLoadError(url=getattr(e, "url", "?"), reason=str(e)) from e
        except Exception as e:
            raise ImageLoadError(url="?", reason=str(e)) from e

    def _do_recognize(self, template, images, request):
        """真实识别流程 (从 rpc/omr_service.py 迁移).

        返回 (answers, abnormal). answers 是 list of dict.
        """
        from omr_service.engine.recognizers.standard import StandardTemplateRecognizer

        recognizer = StandardTemplateRecognizer()
        context = {"template": template, "images": images, "config": request}
        raw = recognizer.recognize(context)
        return raw.get("answers", []), raw.get("abnormal", False)

    def _do_parse(self, image, columns):
        """占位: 模板解析逻辑."""
        # TODO: 接入 engine/standard_template.py
        return [], []
