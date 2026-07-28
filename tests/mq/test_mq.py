"""MQ 消息序列化/反序列化测试"""
import json
import unittest
from unittest.mock import MagicMock

from omr_service.mq.job_handler import BatchJobHandler


class TestMqMessages(unittest.TestCase):
    def test_job_message_parsing(self):
        payload = {
            "job_id": "test-uuid",
            "template_id": 1001,
            "image_urls": ["http://a.jpg", "http://b.jpg"],
            "result_stream": "omr:batch:result",
        }
        s = json.dumps(payload)
        parsed = json.loads(s)
        self.assertEqual(parsed["job_id"], "test-uuid")
        self.assertEqual(len(parsed["image_urls"]), 2)

    def test_job_handler_template_not_found(self):
        """模板不存在且不允许重试时返回错误结果"""
        cfg = MagicMock()
        cfg.redis_result_stream = "omr:batch:result"
        cfg.redis_job_stream = "omr:batch:job"
        cfg.omr_max_retry = 0
        cfg.ocr_confidence_threshold = 0.3
        cfg.crop_output_dir = "./omr_crops"
        cfg.crop_base_url = None
        store = MagicMock()
        store.get.return_value = None
        loader = MagicMock()
        pool = MagicMock()
        pool.submit = lambda fn, *args: MagicMock(result=lambda: fn(*args))
        producer = MagicMock()

        handler = BatchJobHandler(cfg, store, loader, pool, producer)
        job = {"job_id": "j1", "template_id": 1, "image_urls": ["http://x.jpg"]}
        result = handler.handle(job)

        self.assertEqual(result["completed"], 0)
        self.assertEqual(result["failed"], 1)
        producer.send_result.assert_called_once()
        producer.send_job.assert_not_called()


if __name__ == "__main__":
    unittest.main()
