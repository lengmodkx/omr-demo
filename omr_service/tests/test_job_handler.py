"""Job handler 单任务处理单元测试"""
import unittest
from unittest.mock import MagicMock

from omr_service.mq.job_handler import BatchJobHandler
from omr_service.rpc import omr_pb2


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


    def test_parse_golden_template_success(self):
        """黄金模板解析任务成功时应发送结果到结果流"""
        handler = self._make_handler(max_retry=0)
        handler.servicer = unittest.mock.MagicMock()
        handler.servicer.ParseGoldenTemplate.return_value = omr_pb2.GoldenTemplateResult(
            code=0,
            message="ok",
            template_id=1,
            total=2,
            answers={1: "A", 2: "B"},
        )
        job = {
            "job_type": "parse_golden_template",
            "job_id": "j1",
            "template_id": 1,
            "pages": [
                {
                    "template_image_url": "http://page0.jpg",
                    "columns": [
                        {
                            "x1": 0,
                            "y1": 0,
                            "x2": 100,
                            "y2": 200,
                            "startQ": 1,
                            "numQ": 2,
                            "numOptions": 4,
                            "optionAxis": "x",
                            "reverseQ": False,
                        }
                    ],
                }
            ],
        }
        result = handler.handle_parse_golden_template(job)
        self.assertTrue(result["success"])
        self.assertFalse(result["retrying"])
        handler.producer.send_result.assert_called_once()
        payload = handler.producer.send_result.call_args.kwargs["payload"]
        self.assertEqual(payload["job_id"], "j1")
        self.assertEqual(payload["status"], 2)
        self.assertEqual(payload["answers"], {"1": "A", "2": "B"})

    def test_parse_golden_template_permanent_failure_no_retry(self):
        """黄金模板解析遇到永久性错误时不应重试，直接失败"""
        handler = self._make_handler(max_retry=3)
        handler.servicer = unittest.mock.MagicMock()
        handler.servicer.ParseGoldenTemplate.return_value = omr_pb2.GoldenTemplateResult(
            code=5,
            message="图片加载失败",
        )
        job = {
            "job_type": "parse_golden_template",
            "job_id": "j2",
            "template_id": 1,
            "pages": [{"template_image_url": "http://bad.jpg", "columns": []}],
        }
        result = handler.handle_parse_golden_template(job)
        self.assertFalse(result["success"])
        self.assertFalse(result["retrying"])
        handler.producer.send_job.assert_not_called()
        handler.producer.send_result.assert_called_once()


if __name__ == "__main__":
    unittest.main()
