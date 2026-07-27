"""OMR 批量识别 Python 端吞吐量基准脚本（Phase 5）

用法示例：
    cd screenImg
    source .venv/Scripts/activate
    python -m omr_service.benchmarks.batch_throughput --tasks 1000 --workers 4 --handler-sleep-ms 100

说明：
- 本脚本不依赖外部 Redis，直接驱动 consumer._process_batch 进行压力测试。
- handler 可通过 `--handler-sleep-ms` 模拟单张 OCR 耗时；传 0 时测消息分发上限。
- 输出包含：总耗时、吞吐量(tasks/s)、峰值内存(MB)、inflight 峰值。
"""
import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import psutil

# 把仓库根加入路径，保证在 screenImg 内外都能运行
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from omr_service.config import OmrConfig
from omr_service.loader.image_loader import ImageLoader
from omr_service.loader.template_store import TemplateStore
from omr_service.mq.consumer import MqConsumer


def parse_args():
    parser = argparse.ArgumentParser(description="OMR Python 批量识别吞吐量基准")
    parser.add_argument("--tasks", type=int, default=1000, help="模拟任务总数")
    parser.add_argument("--workers", type=int, default=4, help="worker 线程数")
    parser.add_argument("--max-inflight", type=int, default=None, help="最大 inflight 数，默认 workers*2")
    parser.add_argument("--batch-size", type=int, default=10, help="每次 xreadgroup 拉取条数")
    parser.add_argument("--handler-sleep-ms", type=int, default=100, help="模拟单任务处理耗时(ms)，0 表示不睡眠")
    parser.add_argument("--timeout-sec", type=int, default=300, help="整体超时时间")
    return parser.parse_args()


def build_consumer(args):
    cfg = OmrConfig.from_env()
    cfg.omr_max_inflight = args.max_inflight if args.max_inflight else max(8, args.workers * 2)
    cfg.omr_batch_size = args.batch_size
    cfg.omr_single_task_timeout_sec = max(30, args.handler_sleep_ms // 1000 + 10)
    cfg.redis_job_stream = "bench:omr:job"
    cfg.redis_consumer_group = "bench-group"
    cfg.redis_consumer_name = "bench-consumer"

    store = TemplateStore()
    loader = ImageLoader()
    pool = ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="bench_worker")
    consumer = MqConsumer(cfg, store, loader, pool)

    sleep_sec = args.handler_sleep_ms / 1000.0
    inflight_peak = [0]

    def mock_handle(job):
        with consumer._inflight_lock:
            inflight_peak[0] = max(inflight_peak[0], consumer._inflight)
        if sleep_sec > 0:
            time.sleep(sleep_sec)
        return {"success": True}

    handler = MagicMock()
    handler.handle_single_task.side_effect = mock_handle
    consumer._handler = handler
    return consumer, pool, inflight_peak


def build_entries(total: int):
    return [
        (f"0-{i}", {"payload": json.dumps({"task_id": f"T{i}", "batch_id": "B1", "template_id": 1, "image_url": f"http://bench/{i}.jpg"})})
        for i in range(1, total + 1)
    ]


def main():
    args = parse_args()
    print(f"配置: tasks={args.tasks}, workers={args.workers}, max_inflight={args.max_inflight or 'default'}, batch_size={args.batch_size}, sleep_ms={args.handler_sleep_ms}")

    consumer, pool, inflight_peak = build_consumer(args)
    entries = build_entries(args.tasks)
    fake_redis = MagicMock()

    process = psutil.Process()
    mem_before = process.memory_info().rss / 1024 / 1024
    start = time.perf_counter()

    # 按 batch_size 分块喂给 consumer
    batch_size = args.batch_size
    for i in range(0, len(entries), batch_size):
        chunk = entries[i : i + batch_size]
        consumer._process_batch(fake_redis, chunk)

    elapsed = time.perf_counter() - start
    mem_after = process.memory_info().rss / 1024 / 1024

    consumer._stop_event.set()
    pool.shutdown(wait=True)

    throughput = args.tasks / elapsed if elapsed > 0 else 0
    print("\n===== 结果 =====")
    print(f"总耗时:        {elapsed:.2f} s")
    print(f"任务总数:      {args.tasks}")
    print(f"吞吐量:        {throughput:.2f} tasks/s")
    print(f"峰值 inflight: {inflight_peak[0]} (max_allowed={consumer.cfg.omr_max_inflight})")
    print(f"内存 before:   {mem_before:.1f} MB")
    print(f"内存 after:    {mem_after:.1f} MB")
    print(f"内存增量:      {mem_after - mem_before:.1f} MB")

    # 与性能基线对比（单实例 4 worker ≥ 8 张/s）
    baseline = 8.0
    if args.handler_sleep_ms == 0:
        print(f"说明: 当前为无负载消息分发测试，理论上限应远高于 {baseline} tasks/s")
    else:
        status = "[PASS] 达标" if throughput >= baseline else "[FAIL] 未达标"
        print(f"基线对比(单实例 4worker ≥ 8 张/s): {status}")


if __name__ == "__main__":
    main()
