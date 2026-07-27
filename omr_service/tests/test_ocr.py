"""个人信息 OCR 单元测试"""
import unittest

import numpy as np

from omr_service.engine.ocr import PersonalInfoOcr


class FakeOcrEngine:
    """模拟 PaddleOCR 引擎返回结果"""

    def __init__(self, results):
        self.results = results

    def ocr(self, image, cls=True):
        return [self.results]


class TestPersonalInfoOcr(unittest.TestCase):
    def setUp(self):
        self.ocr = PersonalInfoOcr()
        # 重置单例引擎
        self.ocr._ocr_engine = None
        self.image = np.full((100, 200, 3), 255, dtype=np.uint8)

    def test_return_empty_when_engine_unavailable(self):
        """PaddleOCR 初始化失败时应返回空值"""
        self.ocr._ocr_engine = None
        # 强制初始化失败：通过不安装 paddleocr 时 import 失败触发
        # 这里直接模拟引擎为 None
        self.ocr._ocr_engine = None
        regions = [{"field": "name", "x1": 0, "y1": 0, "x2": 50, "y2": 50}]
        results = self.ocr.recognize(self.image, regions)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["field"], "name")
        self.assertEqual(results[0]["value"], "")
        self.assertEqual(results[0]["confidence"], 0.0)

    def test_recognize_single_region(self):
        """正常 OCR 识别单个区域"""
        self.ocr._ocr_engine = FakeOcrEngine([
            (None, ("张三", 0.95)),
        ])
        regions = [{"field": "name", "x1": 0, "y1": 0, "x2": 50, "y2": 50}]
        results = self.ocr.recognize(self.image, regions)
        self.assertEqual(results[0]["value"], "张三")
        self.assertAlmostEqual(results[0]["confidence"], 0.95, places=4)

    def test_recognize_multiple_lines(self):
        """多行 OCR 结果应拼接"""
        self.ocr._ocr_engine = FakeOcrEngine([
            (None, ("张", 0.9)),
            (None, ("三", 0.92)),
        ])
        regions = [{"field": "name", "x1": 0, "y1": 0, "x2": 50, "y2": 50}]
        results = self.ocr.recognize(self.image, regions)
        self.assertEqual(results[0]["value"], "张三")
        self.assertAlmostEqual(results[0]["confidence"], 0.91, places=4)

    def test_invalid_region_returns_empty(self):
        """无效区域（坐标越界）应返回空值"""
        self.ocr._ocr_engine = FakeOcrEngine([])
        regions = [{"field": "name", "x1": 300, "y1": 300, "x2": 400, "y2": 400}]
        results = self.ocr.recognize(self.image, regions)
        self.assertEqual(results[0]["value"], "")


if __name__ == "__main__":
    unittest.main()
