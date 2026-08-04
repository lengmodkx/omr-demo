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
        self.ocr._init_in_progress = False
        self.ocr._init_failed = False
        self.image = np.full((100, 200, 3), 255, dtype=np.uint8)

    def test_return_empty_when_engine_unavailable(self):
        """PaddleOCR 初始化失败时应返回空值"""
        # 模拟引擎不可用（初始化失败返回 None）
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

    def test_get_engine_returns_none_when_init_in_progress(self):
        """初始化进行中（可能挂起）时 _get_engine 应秒回 None，不阻塞任务"""
        import sys
        from unittest.mock import MagicMock, patch

        self.ocr._ocr_engine = None
        self.ocr._init_in_progress = True
        fake_paddleocr = MagicMock()
        try:
            with patch.dict(sys.modules, {"paddleocr": fake_paddleocr}):
                self.assertIsNone(self.ocr._get_engine())
            # 关键断言：初始化进行中时不得触发 import/实例化 PaddleOCR
            fake_paddleocr.PaddleOCR.assert_not_called()
        finally:
            self.ocr._init_in_progress = False

    def test_get_engine_skips_retry_after_failure(self):
        """初始化失败后 _init_failed 置位，后续调用不再重复尝试 import"""
        import sys
        from unittest.mock import MagicMock, patch

        self.ocr._ocr_engine = None
        self.ocr._init_failed = True
        fake_paddleocr = MagicMock()
        try:
            with patch.dict(sys.modules, {"paddleocr": fake_paddleocr}):
                self.assertIsNone(self.ocr._get_engine())
            fake_paddleocr.PaddleOCR.assert_not_called()
        finally:
            self.ocr._init_failed = False

    def test_ocr_calls_are_serialized(self):
        """Paddle 推理非线程安全：多线程并发调用 engine.ocr 必须被串行化（否则偶发返回空）"""
        import threading
        import time

        class RaceDetectEngine:
            def __init__(self):
                self.inflight = 0
                self.max_inflight = 0
                self._lock = threading.Lock()

            def ocr(self, image, cls=True):
                with self._lock:
                    self.inflight += 1
                    self.max_inflight = max(self.max_inflight, self.inflight)
                time.sleep(0.02)  # 模拟推理耗时，放大竞态窗口
                with self._lock:
                    self.inflight -= 1
                return [[(None, ("张三", 0.9))]]

        engine = RaceDetectEngine()
        self.ocr._ocr_engine = engine
        region = {"field": "student_info_block", "x1": 0, "y1": 0, "x2": 50, "y2": 50}

        threads = [
            threading.Thread(target=self.ocr.recognize_block, args=(self.image, region))
            for _ in range(8)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(engine.max_inflight, 1, "engine.ocr 存在并发调用，未被串行化")


if __name__ == "__main__":
    unittest.main()
