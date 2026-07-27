"""gRPC OmrService 实现"""
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

import cv2
import numpy as np

from omr_service.config import OmrConfig
from omr_service.engine.cropper import SubjectiveCropper
from omr_service.engine.ocr import PersonalInfoOcr
from omr_service.engine.personal_info_block_parser import parse_personal_info_block
from omr_service.engine.recognizer import RecognizeContext
from omr_service.engine.recognizers import StandardTemplateRecognizer
from omr_service.engine.standard_template import StandardTemplate
from omr_service.loader.image_loader import ImageLoader
from omr_service.loader.template_store import CachedTemplate, TemplateStore
from omr_service.rpc import omr_pb2
from omr_service.rpc import omr_pb2_grpc
from omr_service.worker.pool import WorkerPool

logger = logging.getLogger(__name__)

CODE_OK = 0
CODE_TEMPLATE_NOT_FOUND = 4
CODE_IMAGE_LOAD_FAIL = 5
CODE_INVALID_REQUEST = 6
CODE_INTERNAL_ERROR = 99

# 考生信息区（整体框选，一次识别姓名/准考证号/考场/座号等）字段标识
STUDENT_INFO_BLOCK_FIELD = "student_info_block"


def _column_config_from_proto(cfg: omr_pb2.ColumnConfig) -> Dict[str, Any]:
    return {
        "x1": cfg.x1,
        "y1": cfg.y1,
        "x2": cfg.x2,
        "y2": cfg.y2,
        "start_q": cfg.start_q,
        "num_q": cfg.num_q,
        "num_options": cfg.num_options,
        "option_axis": cfg.option_axis or "x",
        "reverse_q": cfg.reverse_q,
        "page_index": cfg.page_index,
    }


def _personal_info_from_proto(cfg: omr_pb2.PersonalInfoConfig) -> Dict[str, Any]:
    return {
        "field": cfg.field,
        "x1": cfg.x1,
        "y1": cfg.y1,
        "x2": cfg.x2,
        "y2": cfg.y2,
        "page_index": cfg.page_index,
    }


def _subjective_region_from_proto(cfg: omr_pb2.SubjectiveRegion) -> Dict[str, Any]:
    return {
        "q": cfg.q,
        "x1": cfg.x1,
        "y1": cfg.y1,
        "x2": cfg.x2,
        "y2": cfg.y2,
        "page_index": cfg.page_index,
        "stitch_with_next": cfg.stitch_with_next,
    }


def _bubble_to_proto(b: Dict[str, Any]) -> omr_pb2.Bubble:
    return omr_pb2.Bubble(
        q=b.get("q", 0),
        opt=b.get("opt") or "",
        x=b.get("x", 0),
        y=b.get("y", 0),
        w=b.get("w", 0),
        h=b.get("h", 0),
    )


def _result_to_proto(
    template_id: int,
    image_url: str,
    result: Any,
    personal_info: List[Dict[str, Any]],
    subjective_crops: List[Dict[str, Any]],
    code: int = CODE_OK,
    message: str = "ok",
) -> omr_pb2.RecognizeResult:
    """将识别结果转换为 protobuf"""
    answers_pb = []
    for q, info in result.answers.items():
        answers_pb.append(
            omr_pb2.QuestionAnswer(
                q=q,
                answer=info.get("answer") or "",
                status=info.get("status", "empty"),
                correct=info.get("correct") or False,
            )
        )

    personal_info_pb = [
        omr_pb2.PersonalInfoResult(
            field=p.get("field", ""),
            value=p.get("value", ""),
            confidence=p.get("confidence", 0.0),
        )
        for p in personal_info
    ]

    subjective_crops_pb = [
        omr_pb2.SubjectiveCropResult(
            q=c.get("q", 0),
            image_url=c.get("image_url", ""),
            page_index=c.get("page_index", 0),
        )
        for c in subjective_crops
    ]

    return omr_pb2.RecognizeResult(
        code=code,
        message=message,
        template_id=template_id,
        scan_image_url=image_url,
        answers=answers_pb,
        total=result.total,
        empty_count=result.empty_count,
        multi_count=result.multi_count,
        card_flag=result.card_flag or "",
        duration_ms=int(result.duration_ms),
        personal_info=personal_info_pb,
        subjective_crops=subjective_crops_pb,
    )


class OmrServiceServicer(omr_pb2_grpc.OmrServiceServicer):
    """OMR gRPC 服务实现"""

    def __init__(
        self,
        cfg: OmrConfig,
        template_store: TemplateStore,
        image_loader: ImageLoader,
        worker_pool: WorkerPool,
    ):
        self.cfg = cfg
        self.template_store = template_store
        self.image_loader = image_loader
        self.worker_pool = worker_pool
        self._ocr = PersonalInfoOcr()
        self._cropper = SubjectiveCropper(
            output_dir=cfg.crop_output_dir,
            base_url=cfg.crop_base_url,
        )

    def _get_template(self, template_id: int) -> Optional[CachedTemplate]:
        return self.template_store.get(template_id)

    def _load_image(self, url: str) -> Optional[np.ndarray]:
        return self.image_loader.load(url)

    def _load_images(self, url: str) -> List[np.ndarray]:
        return self.image_loader.load_multi(url)

    def ParseGoldenTemplate(self, request, context):
        """解析黄金模板"""
        logger.info(
            "[rpc] ParseGoldenTemplate template_id=%s url=%s columns=%d personal=%d subjective=%d",
            request.template_id,
            request.template_image_url,
            len(request.columns),
            len(request.personal_info),
            len(request.subjective_regions),
        )

        if request.template_id == 0 or not request.template_image_url:
            return omr_pb2.GoldenTemplateResult(
                code=CODE_INVALID_REQUEST,
                message="template_id / template_image_url 必填",
            )

        # 多页图片：选择题黄金模板用第一页；个人信息/主观题配置全量缓存
        images = self._load_images(request.template_image_url)
        if not images:
            return omr_pb2.GoldenTemplateResult(
                code=CODE_IMAGE_LOAD_FAIL,
                message="模板图片加载失败",
            )

        columns = [_column_config_from_proto(c) for c in request.columns]

        try:
            tpl = StandardTemplate(image=images[0], column_configs=columns)
            # 推断本次请求对应的页码：取所有配置中的 page_index，默认 0
            page_indexes = {c.get("page_index", 0) for c in columns}
            page_indexes.update({_personal_info_from_proto(p).get("page_index", 0) for p in request.personal_info})
            page_indexes.update({_subjective_region_from_proto(s).get("page_index", 0) for s in request.subjective_regions})
            current_page = min(page_indexes) if page_indexes else 0
            cached = CachedTemplate(
                standard_template=tpl,
                personal_info=[_personal_info_from_proto(p) for p in request.personal_info],
                subjective_regions=[_subjective_region_from_proto(s) for s in request.subjective_regions],
                image_url=request.template_image_url,
                page_images={current_page: images[0]} if images else {},
            )
            self.template_store.set(request.template_id, cached)

            bubbles_pb = [_bubble_to_proto(b) for b in tpl.bubbles]

            # 黄金模板阶段可对个人信息区域做示例 OCR（用于确认位置，不强制要求准确）
            personal_info_pb = []
            normal_infos = [
                p for p in (cached.personal_info or [])
                if p.get("field") != STUDENT_INFO_BLOCK_FIELD
            ]
            block_infos = [
                p for p in (cached.personal_info or [])
                if p.get("field") == STUDENT_INFO_BLOCK_FIELD
            ]
            logger.info(
                "[rpc] ParseGoldenTemplate 个人信息拆分: normal=%d block=%d",
                len(normal_infos), len(block_infos),
            )

            if normal_infos and images:
                sample_results = self._ocr.recognize(images[0], normal_infos)
                personal_info_pb.extend([
                    omr_pb2.PersonalInfoResult(
                        field=r.get("field", ""),
                        value=r.get("value", ""),
                        confidence=r.get("confidence", 0.0),
                    )
                    for r in sample_results
                ])

            for block in block_infos:
                if not images:
                    continue
                raw = self._ocr.recognize_block(images[0], block)
                raw_text = raw.get("raw_text", "")
                try:
                    fields, conf = parse_personal_info_block(raw_text)
                except Exception as e:
                    logger.warning("黄金模板考生信息解析异常: %s | raw_text=%s", e, raw_text)
                    fields, conf = {"raw_text": raw_text}, 0.0
                for k, v in fields.items():
                    personal_info_pb.append(omr_pb2.PersonalInfoResult(
                        field=k,
                        value=v,
                        confidence=conf if v else 0.0,
                    ))
                # 临时调试字段，用于确认前端命中的是最新代码
                personal_info_pb.append(omr_pb2.PersonalInfoResult(
                    field="debug_parser_version",
                    value="v2-barcode",
                    confidence=1.0,
                ))

            # 黄金模板阶段示例裁剪主观题区域（确认位置）
            subjective_crops_pb = []
            if cached.subjective_regions and images:
                # Java 端按页拆请求时，每页只传一张图片，但 region 的 page_index 仍是原页码。
                # 这里临时把 page_index 归一化为 0 用于选图，结果里再还原成原始页码。
                single_image = len(images) == 1
                regions_for_parse = []
                original_page_by_q: Dict[int, int] = {}
                for r in cached.subjective_regions:
                    region_copy = dict(r)
                    q = int(region_copy.get("q", 0))
                    original_page_by_q[q] = region_copy.get("page_index", 0)
                    if single_image:
                        region_copy["page_index"] = 0
                    regions_for_parse.append(region_copy)

                sample_crops = self._cropper.crop_subjective_regions(
                    images, regions_for_parse, namespace=f"tpl_{request.template_id}"
                )
                subjective_crops_pb = [
                    omr_pb2.SubjectiveCropResult(
                        q=c.get("q", 0),
                        image_url=c.get("image_url", ""),
                        page_index=original_page_by_q.get(int(c.get("q", 0)), c.get("page_index", 0)),
                    )
                    for c in sample_crops
                ]

            return omr_pb2.GoldenTemplateResult(
                code=CODE_OK,
                message=f"黄金模板解析成功，共 {len(tpl.bubbles)} 个气泡",
                template_id=request.template_id,
                bubbles=bubbles_pb,
                answers={q: (ans or "") for q, ans in tpl.answers.items()},
                total=len(tpl.bubbles),
                personal_info=personal_info_pb,
                subjective_crops=subjective_crops_pb,
            )
        except Exception as e:
            logger.exception("[rpc] ParseGoldenTemplate 失败")
            return omr_pb2.GoldenTemplateResult(
                code=CODE_INTERNAL_ERROR,
                message=f"解析失败: {e}",
            )

    def RecognizeByTemplate(self, request, context):
        """根据模板识别单张答题卡"""
        logger.info("[rpc] RecognizeByTemplate template_id=%s url=%s", request.template_id, request.scan_image_url)
        return self._recognize(request.template_id, request.scan_image_url)

    def ReverifyPaper(self, request, context):
        """单张试卷复验"""
        logger.info("[rpc] ReverifyPaper template_id=%s url=%s", request.template_id, request.scan_image_url)
        return self._recognize(request.template_id, request.scan_image_url)

    def _recognize(self, template_id: int, image_url: str) -> omr_pb2.RecognizeResult:
        if template_id == 0 or not image_url:
            return omr_pb2.RecognizeResult(
                code=CODE_INVALID_REQUEST,
                message="template_id / scan_image_url 必填",
            )

        cached = self._get_template(template_id)
        if cached is None:
            return omr_pb2.RecognizeResult(
                code=CODE_TEMPLATE_NOT_FOUND,
                message="模板未找到，请先调用 ParseGoldenTemplate",
            )

        images = self._load_images(image_url)
        logger.info("[rpc] 答题卡图片加载完成: url=%s pages=%d", image_url, len(images))
        if not images:
            return omr_pb2.RecognizeResult(
                code=CODE_IMAGE_LOAD_FAIL,
                message="答题卡图片加载失败",
                template_id=template_id,
                scan_image_url=image_url,
            )

        try:
            # 选择题识别：使用第一页（Java 端已按 page_index 分组，每页单独发请求）
            recognizer = StandardTemplateRecognizer(standard_template=cached.standard_template)
            ctx = RecognizeContext()
            result = recognizer.recognize(images[0], ctx)

            # 个人信息 OCR
            personal_info_results: List[Dict[str, Any]] = []
            if cached.personal_info and images:
                # 仅对存在且未越界的页面执行 OCR
                valid_images = [img for img in images if img is not None]
                if valid_images:
                    personal_info_results = self._recognize_personal_info(valid_images, cached.personal_info)

            # 主观题裁剪
            subjective_crop_results: List[Dict[str, Any]] = []
            if cached.subjective_regions and images:
                namespace = f"rec_{template_id}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
                subjective_crop_results = self._cropper.crop_subjective_regions(
                    images, cached.subjective_regions, namespace=namespace
                )

            return _result_to_proto(
                template_id,
                image_url,
                result,
                personal_info_results,
                subjective_crop_results,
            )
        except Exception as e:
            logger.exception("[rpc] 识别失败")
            return omr_pb2.RecognizeResult(
                code=CODE_INTERNAL_ERROR,
                message=f"识别失败: {e}",
                template_id=template_id,
                scan_image_url=image_url,
            )

    def _recognize_personal_info(
        self,
        images: List[np.ndarray],
        regions: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """按 page_index 分组图片，批量识别普通个人信息，并单独解析考生信息区整体框。"""
        # 按 page_index 分组，同时保留原始顺序
        page_groups: Dict[int, List[Tuple[int, Dict[str, Any]]]] = {}
        for idx, region in enumerate(regions):
            page_index = int(region.get("page_index", 0))
            page_groups.setdefault(page_index, []).append((idx, region))

        # 预分配结果槽位，普通区域保持与 regions 顺序一致；考生信息区整体框会展开为多个字段
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

            # 普通字段批量识别
            if normal_regions:
                page_results = self._ocr.recognize(image, normal_regions)
                for idx, page_result in zip(normal_indices, page_results):
                    result = page_result if page_result else {"field": normal_regions[normal_indices.index(idx)].get("field", ""), "value": "", "confidence": 0.0}
                    if result.get("confidence", 0.0) < self.cfg.ocr_confidence_threshold:
                        result["value"] = ""
                    results[idx] = result

            # 考生信息区整体框：整框 OCR + 规则解析
            for idx, region in zip(block_indices, block_regions):
                raw_result = self._ocr.recognize_block(image, region)
                raw_text = raw_result.get("raw_text", "")
                try:
                    fields, conf = parse_personal_info_block(raw_text)
                except Exception as e:
                    logger.warning("识别接口考生信息解析异常: %s | raw_text=%s", e, raw_text)
                    fields, conf = {"raw_text": raw_text}, 0.0
                # 以 raw_text 作为占位结果，后续再追加解析出的结构化字段
                results[idx] = {
                    "field": STUDENT_INFO_BLOCK_FIELD,
                    "value": raw_text,
                    "confidence": conf,
                }
                # 将解析出的子字段追加到结果末尾（顺序不重要，前端/后端按 field 读取）
                for k, v in fields.items():
                    if k == "raw_text":
                        continue
                    results.append({
                        "field": k,
                        "value": v,
                        "confidence": conf if v else 0.0,
                    })

        # 兜底
        for idx, region in enumerate(regions):
            if results[idx] is None:
                results[idx] = {"field": region.get("field", ""), "value": "", "confidence": 0.0}
        return results

    def VerifyRecognitionRate(self, request, context):
        """验证黄金模板识别成功率"""
        logger.info(
            "[rpc] VerifyRecognitionRate template_id=%s images=%d",
            request.template_id,
            len(request.image_urls),
        )

        template_id = request.template_id
        if template_id == 0:
            return omr_pb2.VerifyRateResult(
                code=CODE_INVALID_REQUEST,
                message="template_id 必填",
            )

        cached = self._get_template(template_id)
        if cached is None:
            return omr_pb2.VerifyRateResult(
                code=CODE_TEMPLATE_NOT_FOUND,
                message="模板未找到，请先调用 ParseGoldenTemplate",
            )

        expected = dict(request.expected_answers)
        if not expected:
            return omr_pb2.VerifyRateResult(
                code=CODE_INVALID_REQUEST,
                message="expected_answers 不能为空",
            )

        image_urls = list(request.image_urls)
        if not image_urls:
            return omr_pb2.VerifyRateResult(
                code=CODE_INVALID_REQUEST,
                message="image_urls 不能为空",
            )

        def _task(url: str) -> omr_pb2.RecognizeResult:
            return self._recognize(template_id, url)

        # 并发识别
        futures = [self.worker_pool.submit(_task, url) for url in image_urls]
        details: List[omr_pb2.RecognizeResult] = [f.result() for f in futures]

        matched = 0
        for detail in details:
            if detail.code != CODE_OK:
                continue
            actual = {a.q: a.answer for a in detail.answers}
            # 只比较 expected 里的题号
            ok = True
            for q, exp_ans in expected.items():
                if actual.get(q, "") != (exp_ans or ""):
                    ok = False
                    break
            if ok:
                matched += 1

        total = len(details)
        success_rate = matched / total if total > 0 else 0.0

        return omr_pb2.VerifyRateResult(
            code=CODE_OK,
            message="成功率验证完成",
            success_rate=success_rate,
            total=total,
            matched=matched,
            details=details,
        )
