"""Job handler 单任务处理单元测试"""
import unittest
from unittest.mock import MagicMock

from omr_service.mq.job_handler import BatchJobHandler


class TestBatchJobHandler(unittest.TestCase):
    def _make_handler(self, max_retry=3):
        cfg = MagicMock()
        cfg.redis_result_stream = "omr:batch:result"
        cfg.redis_job_stream = "omr:batch:job"
        cfg.omr_max_retry = max_retry
        cfg.ocr_confidence_threshold = 0.3
        cfg.crop_output_dir = "./omr_crops"
        cfg.crop_base_url = None
        store = MagicMock()
        store.get.return_value = None
        loader = MagicMock()
        pool = MagicMock()
        pool.submit = lambda fn, *args: MagicMock(result=lambda: fn(*args))
        producer = MagicMock()
        return BatchJobHandler(cfg, store, loader, pool, producer)

    def test_failed_result_sent_when_no_retry(self):
        handler = self._make_handler(max_retry=0)
        # 模拟模板存在但识别返回失败，由于 max_retry=0 直接失败
        task = {
            "task_id": "t1",
            "batch_id": "b1",
            "template_id": 1,
            "image_url": "http://x.jpg",
            "retry_count": 0,
        }
        result = handler.handle_single_task(task)
        self.assertFalse(result["success"])
        self.assertFalse(result["retrying"])
        handler.producer.send_result.assert_called_once()
        handler.producer.send_job.assert_not_called()

    def test_no_retry_on_permanent_failure(self):
        """模板不存在等永久性错误不应重试，应直接发送失败结果"""
        handler = self._make_handler(max_retry=3)
        task = {
            "task_id": "t2",
            "batch_id": "b2",
            "template_id": 1,
            "image_url": "http://x.jpg",
            "retry_count": 0,
        }
        result = handler.handle_single_task(task)
        self.assertFalse(result["success"])
        self.assertFalse(result["retrying"])
        handler.producer.send_job.assert_not_called()
        handler.producer.send_result.assert_called_once()


if __name__ == "__main__":
    unittest.main()
