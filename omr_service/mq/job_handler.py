"""MQ 批量任务处理（Phase 3：单任务 + 重试 + 死信）"""
import logging
import time
from typing import Any, Dict, List, Optional

import numpy as np

from omr_service.config import OmrConfig
from omr_service.engine.cropper import SubjectiveCropper
from omr_service.engine.ocr import PersonalInfoOcr
from omr_service.engine.recognizer import RecognizeContext
from omr_service.engine.recognizers import StandardTemplateRecognizer
from omr_service.loader.image_loader import ImageLoader
from omr_service.loader.template_store import CachedTemplate, TemplateStore
from omr_service.mq.producer import MqProducer
from omr_service.worker.pool import WorkerPool

logger = logging.getLogger(__name__)


class BatchJobHandler:
    """处理批量识别任务"""

    def __init__(
        self,
        cfg: OmrConfig,
        template_store: TemplateStore,
        image_loader: ImageLoader,
        worker_pool: WorkerPool,
        producer: MqProducer,
    ):
        self.cfg = cfg
        self.template_store = template_store
        self.image_loader = image_loader
        self.worker_pool = worker_pool
        self.producer = producer
        self._ocr = PersonalInfoOcr()
        self._cropper = SubjectiveCropper(
            output_dir=cfg.crop_output_dir,
            base_url=cfg.crop_base_url,
        )

    def _get_or_load_template(self, template_id: int) -> Optional[CachedTemplate]:
        tpl = self.template_store.get(template_id)
        if tpl is not None:
            return tpl
        logger.warning("模板未找到: template_id=%s", template_id)
        return None

    def _recognize_one(
        self,
        template_id: int,
        image_url: str,
        task_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """识别单张（可能多页）答题卡，返回可序列化的 dict"""
        cached = self._get_or_load_template(template_id)
        if cached is None:
            return {
                "scan_image_url": image_url,
                "code": 4,
                "message": "模板未找到",
                "answers": [],
            }

        images = self.image_loader.load_multi(image_url)
        if not images:
            return {
                "scan_image_url": image_url,
                "code": 5,
                "message": "图片加载失败",
                "answers": [],
            }

        try:
            recognizer = StandardTemplateRecognizer(standard_template=cached.standard_template)
            ctx = RecognizeContext()
            result = recognizer.recognize(images[0], ctx)

            personal_info: List[Dict[str, Any]] = []
            if cached.personal_info:
                personal_info = self._recognize_personal_info(images, cached.personal_info)

            subjective_crops: List[Dict[str, Any]] = []
            if cached.subjective_regions:
                namespace = f"task_{task_id or int(time.time() * 1000)}"
                subjective_crops = self._cropper.crop_subjective_regions(
                    images, cached.subjective_regions, namespace=namespace
                )

            return {
                "scan_image_url": image_url,
                "code": 0,
                "message": "ok",
                "template_id": template_id,
                "answers": [
                    {
                        "q": q,
                        "answer": info.get("answer") or "",
                        "status": info.get("status", "empty"),
                        "correct": info.get("correct") or False,
                    }
                    for q, info in result.answers.items()
                ],
                "total": result.total,
                "empty_count": result.empty_count,
                "multi_count": result.multi_count,
                "card_flag": result.card_flag or "",
                "duration_ms": int(result.duration_ms),
                "personal_info": personal_info,
                "subjective_crops": subjective_crops,
            }
        except Exception as e:
            logger.exception("[mq] 单张识别失败: %s", image_url)
            return {"scan_image_url": image_url, "code": 99, "message": str(e), "answers": []}

    def _recognize_personal_info(
        self,
        images: List[np.ndarray],
        regions: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """按 page_index 分组图片，每页只预处理一次并批量识别个人信息区域。"""
        page_groups: Dict[int, List[Tuple[int, Dict[str, Any]]]] = {}
        for idx, region in enumerate(regions):
            page_index = int(region.get("page_index", 0))
            page_groups.setdefault(page_index, []).append((idx, region))

        results: List[Optional[Dict[str, Any]]] = [None] * len(regions)

        for page_index, indexed_regions in page_groups.items():
            if page_index < 0 or page_index >= len(images):
                for idx, region in indexed_regions:
                    results[idx] = {
                        "field": region.get("field", ""),
                        "value": "",
                        "confidence": 0.0,
                    }
                continue

            page_region_list = [region for _, region in indexed_regions]
            page_results = self._ocr.recognize(images[page_index], page_region_list)
            for (idx, region), page_result in zip(indexed_regions, page_results):
                result = page_result if page_result else {"field": region.get("field", ""), "value": "", "confidence": 0.0}
                if result.get("confidence", 0.0) < self.cfg.ocr_confidence_threshold:
                    result["value"] = ""
                results[idx] = result

        for idx, region in enumerate(regions):
            if results[idx] is None:
                results[idx] = {"field": region.get("field", ""), "value": "", "confidence": 0.0}
        return results

    def handle_single_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """处理单条任务，失败时自动重试，达到最大重试后标记 FAILED。

        Args:
            task: 包含 task_id, batch_id, paper_id, template_id, image_url,
                  retry_count, max_retry 的字典。

        Returns:
            {"success": bool, "task_id": str, "retrying": bool}
        """
        task_id = task.get("task_id", "")
        batch_id = task.get("batch_id", "")
        paper_id = task.get("paper_id")
        template_id = int(task.get("template_id", 0))
        image_url = task.get("image_url", "")
        retry_count = int(task.get("retry_count", 0))
        max_retry = int(task.get("max_retry", self.cfg.omr_max_retry))

        try:
            data = self._recognize_one(template_id, image_url, task_id=task_id)
            code = data.get("code", -1)
            if code == 0:
                payload = self._build_success_payload(task, data)
                self.producer.send_result(payload=payload)
                logger.info("[mq] 任务成功 task_id=%s", task_id)
                return {"success": True, "task_id": task_id, "retrying": False}

            # 永久性错误（模板不存在、图片加载失败）直接失败，不再重试
            if code in (4, 5):
                logger.warning(
                    "[mq] 任务遇到永久性错误，直接失败 task_id=%s code=%s message=%s",
                    task_id, code, data.get("message", ""),
                )
                payload = self._build_failure_payload(task, data.get("message", "识别失败"))
                try:
                    self.producer.send_result(payload=payload)
                except Exception as se:
                    logger.error("[mq] 失败结果发送失败 task_id=%s", task_id, exc_info=se)
                return {"success": False, "task_id": task_id, "retrying": False}

            raise RuntimeError(data.get("message", "识别失败"))
        except Exception as e:
            logger.exception("[mq] 任务处理失败 task_id=%s retry=%s", task_id, retry_count)
            retry_count += 1
            if retry_count <= max_retry:
                retry_task = dict(task)
                retry_task["retry_count"] = retry_count
                try:
                    self.producer.send_job(payload=retry_task)
                    logger.info("[mq] 任务重试入队 task_id=%s retry=%s", task_id, retry_count)
                    return {"success": False, "task_id": task_id, "retrying": True}
                except Exception as re:
                    logger.error("[mq] 重试入队失败 task_id=%s", task_id, exc_info=re)
                    retry_count = max_retry + 1

            if retry_count > max_retry:
                payload = self._build_failure_payload(task, str(e))
                try:
                    self.producer.send_result(payload=payload)
                except Exception as se:
                    logger.error("[mq] 失败结果发送失败 task_id=%s", task_id, exc_info=se)
            return {"success": False, "task_id": task_id, "retrying": False}

    def handle(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """兼容旧版聚合消息：拆分为单任务逐个处理。"""
        job_id = job.get("job_id", "")
        template_id = int(job.get("template_id", 0))
        image_url = job.get("image_url")
        image_urls: List[str] = job.get("image_urls", []) or []
        if image_url:
            image_urls = [image_url]

        logger.info("[mq] 开始批量任务 job_id=%s template_id=%s images=%d", job_id, template_id, len(image_urls))
        completed = 0
        failed = 0
        for idx, url in enumerate(image_urls):
            task = {
                "task_id": job.get("task_id") or f"{job_id}_{idx}_{int(time.time() * 1000)}",
                "batch_id": job.get("batch_id", ""),
                "paper_id": job.get("paper_id"),
                "template_id": template_id,
                "image_url": url,
                "retry_count": 0,
                "max_retry": self.cfg.omr_max_retry,
            }
            res = self.handle_single_task(task)
            if res.get("success"):
                completed += 1
            elif not res.get("retrying"):
                failed += 1
        logger.info("[mq] 批量任务完成 job_id=%s completed=%d failed=%d", job_id, completed, failed)
        return {"job_id": job_id, "completed": completed, "failed": failed}

    def _build_success_payload(self, task: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "task_id": task.get("task_id", ""),
            "batch_id": task.get("batch_id", ""),
            "paper_id": task.get("paper_id"),
            "template_id": task.get("template_id", 0),
            "status": 2,
            "scan_image_url": data.get("scan_image_url"),
            "answers": data.get("answers", []),
            "personal_info": data.get("personal_info", []),
            "subjective_crops": data.get("subjective_crops", []),
            "card_flag": data.get("card_flag", ""),
            "duration_ms": data.get("duration_ms", 0),
        }

    def _build_failure_payload(self, task: Dict[str, Any], error_msg: str) -> Dict[str, Any]:
        return {
            "task_id": task.get("task_id", ""),
            "batch_id": task.get("batch_id", ""),
            "paper_id": task.get("paper_id"),
            "template_id": task.get("template_id", 0),
            "status": 3,
            "scan_image_url": task.get("image_url", ""),
            "answers": [],
            "personal_info": [],
            "subjective_crops": [],
            "card_flag": "",
            "duration_ms": 0,
            "error_msg": error_msg,
        }
