"""个人信息 OCR 模块

封装 PaddleOCR 初始化与识别接口，输入为完整答题卡图片 + 区域框，
输出为字段标识、文本内容、置信度。
"""

import logging
import threading
from typing import Any, Dict, List, Optional

import cv2
import numpy as np

logger = logging.getLogger(__name__)


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
            results.append({
                "field": field,
                "value": value,
                "confidence": round(confidence, 4),
            })
        return results

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


def recognize_personal_info(
    image: np.ndarray,
    regions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """便捷函数：识别个人信息区域"""
    return PersonalInfoOcr().recognize(image, regions)
