"""MQ 批量任务处理（Phase 3：单任务 + 重试 + 死信）

Phase 4 (FastAPI 改造): 通过 OmrService（dict-based，与协议无关）调用核心识别/解析。
"""
import logging
import time
from typing import Any, Dict, List, Optional

import numpy as np

from omr_service.config import OmrConfig
from omr_service.core.service import OmrService
from omr_service.engine.cropper import SubjectiveCropper
from omr_service.engine.ocr import PersonalInfoOcr
from omr_service.engine.personal_info_block_parser import parse_personal_info_block
from omr_service.loader.image_loader import ImageLoader
from omr_service.loader.template_store import CachedTemplate, TemplateStore
from omr_service.mq.producer import MqProducer
from omr_service.worker.pool import WorkerPool

# 考生信息区（整体框选，一次识别姓名/准考证号/考场/座号等）字段标识
STUDENT_INFO_BLOCK_FIELD = "student_info_block"

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
        service: Optional[OmrService] = None,
    ):
        self.cfg = cfg
        self.template_store = template_store
        self.image_loader = image_loader
        self.worker_pool = worker_pool
        self.producer = producer
        self.service = service
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
        logger.info("[mq] 答题卡图片加载完成: url=%s pages=%d", image_url, len(images))
        if not images:
            return {
                "scan_image_url": image_url,
                "code": 5,
                "message": "图片加载失败",
                "answers": [],
            }

        try:
            from omr_service.engine.recognizer import RecognizeContext
            from omr_service.engine.recognizers import StandardTemplateRecognizer

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
        """按 page_index 分组图片，批量识别普通个人信息，并单独解析考生信息区整体框。"""
        page_groups: Dict[int, List[Tuple[int, Dict[str, Any]]]] = {}
        for idx, region in enumerate(regions):
            page_index = int(region.get("page_index", 0))
            page_groups.setdefault(page_index, []).append((idx, region))

        results: List[Optional[Dict[str, Any]]] = [None] * len(regions)

        for page_index, indexed_regions in page_groups.items():
            if page_index < 0 or page_index >= len(images):
                for idx, region in indexed_regions:
                    if region.get("field") == STUDENT_INFO_BLOCK_FIELD:
                        results[idx] = {"field": STUDENT_INFO_BLOCK_FIELD, "value": "", "confidence": 0.0}
                    else:
                        results[idx] = {
                            "field": region.get("field", ""),
                            "value": "",
                            "confidence": 0.0,
                        }
                continue

            image = images[page_index]
            normal_regions = []
            normal_indices = []
            block_regions = []
            block_indices = []
            for idx, region in indexed_regions:
                if region.get("field") == STUDENT_INFO_BLOCK_FIELD:
                    block_regions.append(region)
                    block_indices.append(idx)
                else:
                    normal_regions.append(region)
                    normal_indices.append(idx)

            if normal_regions:
                page_results = self._ocr.recognize(image, normal_regions)
                for idx, page_result in zip(normal_indices, page_results):
                    result = page_result if page_result else {"field": normal_regions[normal_indices.index(idx)].get("field", ""), "value": "", "confidence": 0.0}
                    if result.get("confidence", 0.0) < self.cfg.ocr_confidence_threshold:
                        result["value"] = ""
                    results[idx] = result

            for idx, region in zip(block_indices, block_regions):
                raw_result = self._ocr.recognize_block(image, region)
                raw_text = raw_result.get("raw_text", "")
                try:
                    fields, conf = parse_personal_info_block(raw_text)
                except Exception as e:
                    logger.warning("考生信息解析异常，保留原始文本: %s | raw_text=%s", e, raw_text)
                    fields, conf = {"raw_text": raw_text}, 0.0
                results[idx] = {
                    "field": STUDENT_INFO_BLOCK_FIELD,
                    "value": raw_text,
                    "confidence": conf,
                }
                for k, v in fields.items():
                    if k == "raw_text":
                        continue
                    results.append({
                        "field": k,
                        "value": v,
                        "confidence": conf if v else 0.0,
                    })

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

    def handle_parse_golden_template(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """处理整张试卷模板的黄金模板解析任务（可能多页）。

        Args:
            job: 包含 job_id, template_id, pages, retry_count, max_retry 的字典。

        Returns:
            {"success": bool, "job_id": str, "retrying": bool}
        """
        job_id = job.get("job_id", "")
        template_id = int(job.get("template_id", 0))
        retry_count = int(job.get("retry_count", 0))
        max_retry = int(job.get("max_retry", self.cfg.omr_max_retry))

        try:
            data = self._do_parse_golden_template(job)
            code = data.get("code", -1)
            if code == 0:
                payload = self._build_parse_success_payload(job_id, template_id, data)
                self.producer.send_result(payload=payload)
                logger.info("[mq] 黄金模板解析任务成功 job_id=%s", job_id)
                return {"success": True, "job_id": job_id, "retrying": False}

            # 永久性错误（请求无效、图片加载失败）直接失败，不再重试
            if code in (4, 5, 6):
                logger.warning(
                    "[mq] 黄金模板解析遇到永久性错误，直接失败 job_id=%s code=%s message=%s",
                    job_id, code, data.get("message", ""),
                )
                payload = self._build_parse_failure_payload(
                    job_id, template_id, data.get("message", "黄金模板解析失败")
                )
                self.producer.send_result(payload=payload)
                return {"success": False, "job_id": job_id, "retrying": False}

            raise RuntimeError(data.get("message", "黄金模板解析失败"))
        except Exception as e:
            logger.exception("[mq] 黄金模板解析任务失败 job_id=%s retry=%s", job_id, retry_count)
            retry_count += 1
            if retry_count <= max_retry:
                retry_job = dict(job)
                retry_job["retry_count"] = retry_count
                try:
                    self.producer.send_job(payload=retry_job)
                    logger.info("[mq] 黄金模板解析任务重试入队 job_id=%s retry=%s", job_id, retry_count)
                    return {"success": False, "job_id": job_id, "retrying": True}
                except Exception as re:
                    logger.error("[mq] 黄金模板解析重试入队失败 job_id=%s", job_id, exc_info=re)
                    retry_count = max_retry + 1

            if retry_count > max_retry:
                payload = self._build_parse_failure_payload(job_id, template_id, str(e))
                try:
                    self.producer.send_result(payload=payload)
                except Exception as se:
                    logger.error("[mq] 黄金模板解析失败结果发送失败 job_id=%s", job_id, exc_info=se)
            return {"success": False, "job_id": job_id, "retrying": False}

    def _do_parse_golden_template(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """逐页调用 OmrService.parse_golden_template 并合并结果。"""
        template_id = int(job.get("template_id", 0))
        pages: List[Dict[str, Any]] = job.get("pages", []) or []
        if not pages:
            return {"code": 6, "message": "pages 不能为空"}

        if self.service is None:
            return {"code": 99, "message": "service 未初始化，无法解析黄金模板"}

        merged_answers: Dict[str, str] = {}
        merged_bubbles: List[Dict[str, Any]] = []
        merged_personal_info: List[Dict[str, Any]] = []
        merged_subjective_crops: List[Dict[str, Any]] = []
        total = 0

        for page in pages:
            request = self._build_parse_request(template_id, page)
            try:
                resp_dict = self.service.parse_golden_template(request)
            except Exception as exc:
                logger.exception("[mq] 黄金模板解析异常 page=%s", page.get("page_index"))
                return {"code": 99, "message": str(exc)}

            code = resp_dict.get("code", -1)
            if code != 0:
                return {
                    "code": code,
                    "message": resp_dict.get("message", "第 {} 页黄金模板解析失败".format(page.get("page_index", 0) + 1)),
                }

            page_answers = resp_dict.get("answers", []) or []
            # 新 OmrService 返回 list；旧协议可能返回 dict。统一合并为 dict。
            if isinstance(page_answers, dict):
                merged_answers.update(page_answers)
            else:
                for ans in page_answers:
                    q_no = ans.get("question_no") if isinstance(ans, dict) else None
                    if q_no is None:
                        continue
                    selected = ans.get("selected", []) if isinstance(ans, dict) else []
                    merged_answers[str(q_no)] = ",".join(selected) if selected else ""
            bubble_grid = resp_dict.get("bubble_grid") or []
            for b in bubble_grid:
                merged_bubbles.append(b)
            personal_info_sample = resp_dict.get("personal_info_sample")
            if personal_info_sample is not None:
                merged_personal_info.append(personal_info_sample)
            subjective_crops = resp_dict.get("subjective_crops") or []
            merged_subjective_crops.extend(subjective_crops)
            total += len(page_answers) if isinstance(page_answers, list) else len(page_answers)

        return {
            "code": 0,
            "message": "ok",
            "template_id": template_id,
            "answers": merged_answers,
            "bubbles": merged_bubbles,
            "personal_info": merged_personal_info,
            "subjective_crops": merged_subjective_crops,
            "total": total,
            "total_images": len(pages),
            "success_count": len(pages),
            "failed_count": 0,
        }

    @staticmethod
    def _build_parse_request(template_id: int, page: Dict[str, Any]) -> Dict[str, Any]:
        """将 MQ 任务的 page 信息转换为 OmrService.parse_golden_template 的输入 dict."""
        columns_raw = page.get("columns", []) or []
        columns: List[Dict[str, Any]] = []
        for idx, c in enumerate(columns_raw):
            # 兼容两层入参：MQ 旧字段名 (x1/y1/x2/y2/startQ/numQ/numOptions)
            # 与 OmrService 期望字段 (column_id/column_index/question_start/question_count/options_per_question)
            columns.append({
                "column_id": c.get("column_id") or c.get("id") or f"col_{idx}",
                "column_index": int(c.get("column_index", idx)),
                "question_start": int(c.get("question_start", c.get("startQ", c.get("start_q", 1)))),
                "question_count": int(c.get("question_count", c.get("numQ", c.get("num_q", 0)))),
                "options_per_question": int(c.get("options_per_question", c.get("numOptions", c.get("num_options", 4)))),
                "question_type": c.get("question_type", "single"),
            })

        request: Dict[str, Any] = {
            "template_id": template_id,
            "template_image_url": page.get("template_image_url") or page.get("templateImageUrl"),
            "columns": columns,
        }
        if page.get("personal_info_region"):
            request["personal_info_region"] = page["personal_info_region"]
        if page.get("subjective_regions"):
            request["subjective_regions"] = page["subjective_regions"]
        return request

    def _build_parse_success_payload(
        self,
        job_id: str,
        template_id: int,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "job_id": job_id,
            "job_type": "parse_golden_template",
            "template_id": template_id,
            "status": 2,
            "answers": data.get("answers", {}),
            "bubbles": data.get("bubbles", []),
            "personal_info": data.get("personal_info", []),
            "subjective_crops": data.get("subjective_crops", []),
            "total": data.get("total", 0),
            "total_images": data.get("total_images", 1),
            "success_count": data.get("success_count", 1),
            "failed_count": data.get("failed_count", 0),
        }

    def _build_parse_failure_payload(
        self,
        job_id: str,
        template_id: int,
        error_msg: str,
    ) -> Dict[str, Any]:
        return {
            "job_id": job_id,
            "job_type": "parse_golden_template",
            "template_id": template_id,
            "status": 3,
            "error_msg": error_msg,
            "answers": {},
            "bubbles": [],
            "personal_info": [],
            "subjective_crops": [],
            "total": 0,
            "total_images": 0,
            "success_count": 0,
            "failed_count": 1,
        }

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
