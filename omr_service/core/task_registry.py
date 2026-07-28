"""异步任务结果 Hash 读取.

与 mq/producer.py 配合: producer 写任务 (XADD omr:batch:job) 时同时写 Hash 标记 processing;
job_handler 完成时更新 Hash 标记 succeeded/failed.
"""
from __future__ import annotations

import json
from typing import Any

from omr_service.api.schemas.enums import TaskStatus
from omr_service.core.exceptions import TaskNotFoundError


class TaskRegistry:
    def __init__(self, redis_client, hash_prefix: str):
        self.redis = redis_client
        self.hash_prefix = hash_prefix

    def _key(self, task_id: str) -> str:
        return f"{self.hash_prefix}:{task_id}"

    def get(self, task_id: str) -> dict[str, Any]:
        raw = self.redis.hgetall(self._key(task_id))
        if not raw:
            raise TaskNotFoundError(task_id)

        result_str = raw.get("result")
        error_str = raw.get("error")
        return {
            "task_id": task_id,
            "task_type": raw.get("task_type"),
            "status": TaskStatus(raw.get("status", "queued")),
            "created_at": raw.get("created_at"),
            "finished_at": raw.get("finished_at"),
            "result": json.loads(result_str) if result_str else None,
            "error": json.loads(error_str) if error_str else None,
        }

    def write_queued(self, task_id: str, task_type: str, payload: dict, created_at: str) -> None:
        """任务入队时由 producer 调用."""
        self.redis.hset(self._key(task_id), mapping={
            "status": TaskStatus.QUEUED.value,
            "task_type": task_type,
            "created_at": created_at,
            "payload": json.dumps(payload),
        })

    def write_processing(self, task_id: str) -> None:
        self.redis.hset(self._key(task_id), "status", TaskStatus.PROCESSING.value)

    def write_succeeded(self, task_id: str, result: dict, finished_at: str) -> None:
        self.redis.hset(self._key(task_id), mapping={
            "status": TaskStatus.SUCCEEDED.value,
            "result": json.dumps(result),
            "finished_at": finished_at,
        })

    def write_failed(self, task_id: str, error: dict, finished_at: str) -> None:
        self.redis.hset(self._key(task_id), mapping={
            "status": TaskStatus.FAILED.value,
            "error": json.dumps(error),
            "finished_at": finished_at,
        })