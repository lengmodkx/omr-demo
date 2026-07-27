"""Redis Stream Consumer 单元测试（Phase 5：背压与并发）"""
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

from omr_service.config import OmrConfig
from omr_service.loader.image_loader import ImageLoader
from omr_service.loader.template_store import TemplateStore
from omr_service.mq.consumer import MqConsumer


class TestMqConsumer(unittest.TestCase):
    def _make_consumer(self, max_inflight: int = 2, batch_size: int = 2):
        cfg = OmrConfig.from_env()
        cfg.omr_max_inflight = max_inflight
        cfg.omr_batch_size = batch_size
        cfg.omr_single_task_timeout_sec = 5
        cfg.redis_job_stream = "test:omr:job"
        cfg.redis_consumer_group = "test-group"
        cfg.redis_consumer_name = "test-consumer"

        store = TemplateStore()
        loader = ImageLoader()
        pool = ThreadPoolExecutor(max_workers=max_inflight)
        consumer = MqConsumer(cfg, store, loader, pool)
        # 替换 handler 为快速 mock，避免真实 OCR
        handler = MagicMock()
        handler.handle_single_task.return_value = {"success": True}
        consumer._handler = handler
        return consumer, pool

    def test_wait_inflight_slot_blocks_until_below_capacity(self):
        """inflight 达到上限时 _wait_inflight_slot 应阻塞，释放后继续"""
        consumer, pool = self._make_consumer(max_inflight=2)
        consumer._inflight = 2
        released = threading.Event()

        def wait_and_signal():
            consumer._wait_inflight_slot()
            released.set()

        t = threading.Thread(target=wait_and_signal, daemon=True)
        t.start()
        # 等待期间不应释放
        self.assertFalse(released.wait(timeout=0.2))
        consumer._inflight = 1
        # 仍高于等于 max_inflight? max=2, inflight=1 < 2 => 应释放
        self.assertTrue(released.wait(timeout=1.0))
        consumer._stop_event.set()
        pool.shutdown(wait=False)

    def test_process_batch_acks_successful_jobs(self):
        """成功任务应调用 xack + xdel，且 inflight 最终归零"""
        consumer, pool = self._make_consumer(max_inflight=10, batch_size=3)
        fake_redis = MagicMock()
        entries = [
            ("0-1", {"payload": '{"task_id":"t1"}'}),
            ("0-2", {"payload": '{"task_id":"t2"}'}),
            ("0-3", {"payload": '{"task_id":"t3"}'}),
        ]
        consumer._process_batch(fake_redis, entries)

        self.assertEqual(consumer._inflight, 0)
        self.assertEqual(fake_redis.xack.call_count, 3)
        self.assertEqual(fake_redis.xdel.call_count, 3)
        consumer._stop_event.set()
        pool.shutdown(wait=False)

    def test_process_batch_does_not_ack_failed_jobs(self):
        """失败任务不 ack，保留 pending 供重试"""
        consumer, pool = self._make_consumer(max_inflight=10, batch_size=2)
        consumer._handler.handle_single_task.return_value = {"success": False}
        fake_redis = MagicMock()
        entries = [
            ("0-1", {"payload": '{"task_id":"t1"}'}),
            ("0-2", {"payload": '{"task_id":"t2"}'}),
        ]
        consumer._process_batch(fake_redis, entries)

        self.assertEqual(consumer._inflight, 0)
        fake_redis.xack.assert_not_called()
        fake_redis.xdel.assert_not_called()
        consumer._stop_event.set()
        pool.shutdown(wait=False)

    def test_inflight_never_exceeds_max_across_batches(self):
        """连续处理多批消息时，inflight 峰值不超过 max_inflight"""
        consumer, pool = self._make_consumer(max_inflight=3, batch_size=2)
        consumer._handler.handle_single_task.return_value = {"success": True}
        fake_redis = MagicMock()
        max_observed = [0]

        def delayed_handle(job):
            with consumer._inflight_lock:
                max_observed[0] = max(max_observed[0], consumer._inflight)
            time.sleep(0.05)
            return {"success": True}

        consumer._handler.handle_single_task.side_effect = delayed_handle

        # 模拟两批消息
        batch1 = [
            ("0-1", {"payload": '{"task_id":"t1"}'}),
            ("0-2", {"payload": '{"task_id":"t2"}'}),
        ]
        batch2 = [
            ("0-3", {"payload": '{"task_id":"t3"}'}),
            ("0-4", {"payload": '{"task_id":"t4"}'}),
        ]
        consumer._process_batch(fake_redis, batch1)
        consumer._process_batch(fake_redis, batch2)

        self.assertLessEqual(max_observed[0], 3)
        self.assertEqual(consumer._inflight, 0)
        self.assertEqual(fake_redis.xack.call_count, 4)
        consumer._stop_event.set()
        pool.shutdown(wait=False)

    def test_stop_waits_for_inflight(self):
        """stop 应等待 inflight 任务完成"""
        consumer, pool = self._make_consumer(max_inflight=5, batch_size=1)
        consumer._inflight = 1

        def finish_after_delay():
            time.sleep(0.2)
            consumer._inflight = 0

        t = threading.Thread(target=finish_after_delay, daemon=True)
        t.start()

        start = time.time()
        consumer.stop()
        elapsed = time.time() - start

        self.assertGreaterEqual(elapsed, 0.15)
        self.assertEqual(consumer._inflight, 0)
        pool.shutdown(wait=False)


if __name__ == "__main__":
    unittest.main()
