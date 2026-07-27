"""个人信息 OCR 模块

封装 PaddleOCR 初始化与识别接口，输入为完整答题卡图片 + 区域框，
输出为字段标识、文本内容、置信度。
"""

import logging
import re
import threading
from typing import Any, Dict, List, Optional



import cv2
import numpy as np

logger = logging.getLogger(__name__)

# 常见准考证号/考生号类字段标识，识别后需要兜底提取纯数字
_STUDENT_ID_FIELDS = {
    "student_no",
    "admission_no",
    "exam_no",
    "candidate_no",
    "id_number",
    "准考证号",
    "准考证号码",
    "考号",
    "学号",
    "考生号",
    "报名号",
}


class PersonalInfoOcr:
    """基于 PaddleOCR 的个人信息识别器（懒加载单例，线程安全）"""

    _instance: Optional["PersonalInfoOcr"] = None
    _ocr_engine: Any = None
    _init_lock = threading.Lock()

    def __new__(cls) -> "PersonalInfoOcr":
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def _get_engine(self) -> Any:
        """懒加载 PaddleOCR 引擎，初始化失败时返回 None 并记录日志（线程安全）"""
        if self._ocr_engine is not None:
            return self._ocr_engine
        with self._init_lock:
            # 双重检查，防止多个线程重复初始化
            if self._ocr_engine is not None:
                return self._ocr_engine
            try:
                from paddleocr import PaddleOCR

                self._ocr_engine = PaddleOCR(
                    use_angle_cls=True,
                    lang="ch",
                    show_log=False,
                    enable_mkldnn=False,
                )
                logger.info("PaddleOCR 初始化成功")
            except Exception as e:
                logger.warning("PaddleOCR 初始化失败，个人信息 OCR 将不可用: %s", e)
                self._ocr_engine = None
        return self._ocr_engine

    def recognize(
        self,
        image: np.ndarray,
        regions: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """识别多个个人信息区域

        Args:
            image: 完整答题卡图片（BGR）
            regions: 区域列表，每项包含 field, x1, y1, x2, y2

        Returns:
            识别结果列表，每项包含 field, value, confidence
        """
        engine = self._get_engine()
        if engine is None:
            return [
                {"field": r.get("field", ""), "value": "", "confidence": 0.0}
                for r in regions
            ]

        results = []
        for region in regions:
            field = region.get("field", "")
            crop = self._crop(image, region)
            if crop.size == 0:
                results.append({"field": field, "value": "", "confidence": 0.0})
                continue
            preprocessed = self._preprocess(crop)
            value, confidence = self._recognize_one(engine, preprocessed)
            # 准考证号等字段：OCR 容易把标签和数字连在一起；同时尝试条码解码兜底
            if field and self._is_student_id_field(field):
                barcode_texts = self._try_decode_barcodes(crop)
                if barcode_texts:
                    combined = (value + "\n" + "\n".join(barcode_texts)).strip()
                    logger.info(
                        "[ocr] 字段 %s 条码解码结果: %s, 与 OCR 合并: '%s'",
                        field, barcode_texts, combined,
                    )
                    value = combined
                extracted = self._extract_id_number(value)
                if extracted and extracted != value:
                    logger.info(
                        "[ocr] 字段 %s 原值 '%s' 包含非数字内容，提取准考证号: %s",
                        field, value, extracted,
                    )
                    value = extracted
            results.append({
                "field": field,
                "value": value,
                "confidence": round(confidence, 4),
            })
        return results

    @staticmethod
    def _try_decode_barcodes(image: np.ndarray) -> List[str]:
        """尝试识别图片中的条形码/二维码，返回解码字符串列表"""
        try:
            from pyzbar.pyzbar import decode

            barcodes = decode(image)
            results = []
            for barcode in barcodes:
                data = barcode.data.decode("utf-8") if barcode.data else ""
                if data:
                    results.append(data)
            return results
        except Exception as e:
            logger.debug("条码解码异常: %s", e)
            return []

    @staticmethod
    def _is_student_id_field(field: str) -> bool:
        """判断字段是否为准考证号/考生号类字段"""
        if not field:
            return False
        key = field.strip().lower().replace(" ", "_")
        return key in _STUDENT_ID_FIELDS or any(
            token in key for token in ("student", "admission", "exam", "candidate", "准考证", "考号", "学号", "考生号", "报名号")
        )

    @staticmethod
    def _extract_id_number(text: str) -> str:
        """从文本中提取最长的 6-20 位数字串，用于准考证号兜底"""
        if not text:
            return ""
        nums = re.findall(r"\d{6,20}", text)
        return max(nums, key=len) if nums else ""

    @staticmethod
    def _crop(image: np.ndarray, region: Dict[str, Any]) -> np.ndarray:
        """按区域裁剪图片"""
        h, w = image.shape[:2]
        x1 = max(0, min(int(region.get("x1", 0)), w - 1))
        y1 = max(0, min(int(region.get("y1", 0)), h - 1))
        x2 = max(x1 + 1, min(int(region.get("x2", x1 + 1)), w))
        y2 = max(y1 + 1, min(int(region.get("y2", y1 + 1)), h))
        return image[y1:y2, x1:x2]

    @staticmethod
    def _preprocess(image: np.ndarray) -> np.ndarray:
        """OCR 前预处理：灰度、CLAHE 增强、去噪"""
        if len(image.shape) == 3:
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        else:
            gray = image
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        enhanced = clahe.apply(gray)
        denoised = cv2.fastNlMeansDenoising(enhanced, None, 10, 7, 21)
        return denoised

    @staticmethod
    def _recognize_one(engine: Any, image: np.ndarray) -> tuple[str, float]:
        """单张图片 OCR，返回 (文本, 平均置信度)"""
        try:
            result = engine.ocr(image, cls=True)
        except Exception as e:
            logger.warning("OCR 识别异常: %s", e)
            return "", 0.0

        # PaddleOCR 返回结构：[[[box], (text, confidence)], ...]
        if not result or not result[0]:
            return "", 0.0

        texts = []
        confidences = []
        for line in result[0]:
            if line and len(line) == 2:
                _, (text, conf) = line
                texts.append(text or "")
                confidences.append(float(conf) if conf is not None else 0.0)

        full_text = "".join(texts).strip().replace(" ", "")
        avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
        return full_text, avg_conf


    def recognize_block(
        self,
        image: np.ndarray,
        region: Dict[str, Any],
    ) -> Dict[str, Any]:
        """识别整块的考生信息区，保留换行与空格，返回原始文本和平均置信度。

        Args:
            image: 完整答题卡图片（BGR）
            region: 区域配置，包含 x1, y1, x2, y2

        Returns:
            {"raw_text": str, "confidence": float}
        """
        engine = self._get_engine()
        crop = self._crop(image, region)
        if crop.size == 0:
            return {"raw_text": "", "confidence": 0.0}

        if engine is None:
            return {"raw_text": "", "confidence": 0.0}

        preprocessed = self._preprocess(crop)
        try:
            result = engine.ocr(preprocessed, cls=True)
        except Exception as e:
            logger.warning("考生信息区 OCR 异常: %s", e)
            return {"raw_text": "", "confidence": 0.0}

        if not result or not result[0]:
            return {"raw_text": "", "confidence": 0.0}

        texts = []
        confidences = []
        for line in result[0]:
            if line and len(line) == 2:
                _, (text, conf) = line
                if text:
                    texts.append(text)
                confidences.append(float(conf) if conf is not None else 0.0)

        # 尝试条码解码（准考证号常为条形码），把解码结果追加到文本末尾供后续规则提取
        try:
            from pyzbar.pyzbar import decode

            barcodes = decode(crop)
            for barcode in barcodes:
                data = barcode.data.decode("utf-8") if barcode.data else ""
                if data:
                    texts.append(data)
                    confidences.append(1.0)
        except Exception as e:
            logger.debug("考生信息区条码解码异常: %s", e)

        # 按 PaddleOCR 返回的行顺序拼接，保留换行
        raw_text = "\n".join(texts).strip()
        avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
        return {"raw_text": raw_text, "confidence": round(avg_conf, 4)}


def recognize_personal_info(
    image: np.ndarray,
    regions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """便捷函数：识别个人信息区域"""
    return PersonalInfoOcr().recognize(image, regions)


def recognize_personal_info_block(
    image: np.ndarray,
    region: Dict[str, Any],
) -> Dict[str, Any]:
    """便捷函数：识别考生信息区整体"""
    return PersonalInfoOcr().recognize_block(image, region)
