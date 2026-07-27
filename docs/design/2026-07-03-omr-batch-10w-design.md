# OMR 阅卷批量识别 10w+ 量级设计（精简版）

> **完整版**：[`C:\Users\lengm\.claude\brainstorm-docs\projects\2026-07-03-omr-batch-10w-design.md`](file:///C:/Users/lengm/.claude/brainstorm-docs/projects/2026-07-03-omr-batch-10w-design.md)
>
> 本文档是 OMR 服务仓库内的精简副本，仅保留本服务相关章节，方便和代码一起查阅。

## 1. 现状与问题

| 缺陷 | 10w+ 量级影响 |
|------|---------------|
| `BatchJobHandler.handle()` 一次性 submit 全部图片 future | **OMR 端 OOM** |
| 1 个 job 包含所有图片，无拆分 | 无法分片并行，进度无法精确查询 |
| 无任务持久化（PEL 内消息无业务上下文） | Redis 重启即丢任务 |
| 无成功率统计 | 教育局无法看到统考质量 |
| 单消费者组单流模式 | 多机扩展受限 |

## 2. 目标

| 指标 | 目标 |
|------|------|
| 单批次处理能力 | ≤ 50w 张 |
| 并发实例数 | 1 ~ N 线性扩展 |
| OMR 端内存峰值 | ≤ 1GB（4 worker） |
| 进度查询响应 | ≤ 500ms |
| 任务可靠投递 | ≥ 99.99% |
| 消息堆积 | ≤ 100w 条 |

**不引入新中间件**，继续使用 Redis Stream。

## 3. 核心改造

### 3.1 任务拆分（Java 端）

`1 张图片 1 条消息`，按 paperId 排序后批量 Pipeline xadd：

```java
// 100k 张 → 100 次 Pipeline，每次 1000 条
ExamOmrBatchSplitter.splitAndSend(batchId, templateId, papers, 1000);
```

### 3.2 背压消费（Python 端）

```python
# 限制 inflight ≤ 2×worker_count
self._max_inflight = cfg.omr_max_inflight  # 默认 8

with self._inflight_cond:
    while self._inflight >= self._max_inflight:
        self._inflight_cond.wait(timeout=2.0)
```

**关键改动**：[`omr_service/mq/consumer.py`](../../omr_service/mq/consumer.py) 重构为流式，`job_handler.py` 拆 `handle` → `handle_single_task`。

### 3.3 重试机制

```python
# 单任务失败时
if retry_count < max_retry:
    self._requeue_with_increment_retry(task)  # 1s 后重新 xadd
else:
    # 标记 FAILED 写入结果流
```

### 3.4 进度持久化

| 表 | 作用 |
|----|------|
| `exam_omr_batch` | 主任务状态 / 进度聚合 |
| `exam_omr_task` | 子任务状态（task_id = msg_id） |
| `exam_paper_omr_result` | 最终识别结果 |
| `exam_omr_success_rate` | 每日成功率统计 |

详见完整版 §3.1。

## 4. 消息格式

### 4.1 任务消息（`omr:batch:job`）

```json
{
  "task_id": "1830123456789012345",
  "batch_id": "1830123456789010000",
  "paper_id": "1830123450000001234",
  "template_id": "1950000000000000001",
  "image_url": "https://minio.xxx/paper/xxx.jpg",
  "max_retry": "3"
}
```

### 4.2 结果消息（`omr:batch:result`）

```json
{
  "task_id": "1830123456789012345",
  "batch_id": "1830123456789010000",
  "code": "0",
  "answers_json": "[{\"q\":1,\"answer\":\"A\"}]",
  "card_flag": "A",
  "total": 50,
  "empty_count": 2,
  "multi_count": 0,
  "retry_count": "0",
  "omr_instance_ip": "192.168.1.20",
  "duration_ms": "456"
}
```

## 5. OMR 端配置新增

```python
# omr_service/config.py 新增
omr_max_inflight: int = field(
    default_factory=lambda: max(8, (os.cpu_count() or 4) * 2)
)
omr_batch_size: int = 10
omr_max_retry: int = 3
omr_retry_delay_sec: int = 1
```

## 6. 性能基线

| 指标 | 单实例 (4w) | 3 实例 (12w) | 6 实例 (24w) |
|------|------------|--------------|--------------|
| 处理能力 | 8 张/s | 24 张/s | 48 张/s |
| 10w 张耗时 | 3.5h | 70min | 35min |
| 内存峰值 | 200MB | 600MB | 1.2GB |

## 7. 可靠性

| 失效 | 应对 |
|------|------|
| OMR 崩溃 | PEL 自动保留，重启继续消费 |
| Redis 重启 | **必须开 AOF**，否则数据丢失 |
| Java Consumer 崩溃 | 超时扫描 `status=PROCESSING AND start_time < NOW-5min` 重置为 PENDING |
| 重复消费 | `exam_omr_task.task_id` 唯一约束 + `INSERT IGNORE` |
| 失败重试风暴 | `max_retry=3` + 重试延迟 1s |

## 8. 演进路径

| 阶段 | 时间 | 触发条件 | 目标 |
|------|------|----------|------|
| 1 | 当前 | 已有 | Redis Stream + 任务拆分 + 背压 |
| 2 | 3-6 月 | 日均 > 50w 任务 | 加 Prometheus 监控 + 死信队列 |
| 3 | 1 年+ | 盟市 > 10 / 堆积 > 100w | **迁移到 RocketMQ** |

## 9. 实施 TODO

详见 `todos/2026-07-03-omr-batch-10w-implementation.md`（仓库根目录）。
