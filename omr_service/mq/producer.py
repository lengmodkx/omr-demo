"""Redis Stream 结果生产者"""
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from omr_service.config import OmrConfig
from omr_service.mq.client import MqClient

logger = logging.getLogger(__name__)


def enqueue_job(
    task_type: str,
    payload: dict,
    task_id: Optional[str] = None,
    *,
    hash_prefix: str = "omr:batch:result:hash",
    cfg: Optional[OmrConfig] = None,
) -> str:
    """便捷函数: 把异步任务投递到 Redis Stream + 写 Hash 标记 queued.

    由 API router 调用, 0 改动 OmrConfig / consumer / job_handler.

    双写契约 (Plan A Task 16):
      1. Stream: MqProducer.send_job(payload={"task_id", "job_type", **payload})
      2. Hash:   redis.hset(f"{hash_prefix}:{task_id}", mapping={status, task_type, ...})

    Args:
        task_type: 任务类型 (recognize / crop / ...)
        payload: 业务负载, 函数内部会合并 task_id 和 job_type
        task_id: 可选外部 task_id, 为 None 时自动生成 uuid4
        hash_prefix: Redis Hash key 前缀
        cfg: 可选注入配置 (主要为测试)

    Returns:
        task_id (str)
    """
    effective_cfg = cfg or OmrConfig.from_env()

    if not task_id:
        task_id = str(uuid.uuid4())

    # 1. Stream 写
    producer = MqProducer(effective_cfg).connect()
    try:
        body = {"task_id": task_id, "job_type": task_type, **payload}
        producer.send_job(payload=body)
    finally:
        producer.close()

    # 2. Hash 写
    hash_client = MqClient(effective_cfg).connect()
    try:
        hash_client.redis.hset(
            f"{hash_prefix}:{task_id}",
            mapping={
                "status": "queued",
                "task_type": task_type,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "payload": json.dumps(payload, ensure_ascii=False, default=str),
            },
        )
    except Exception as e:
        # Hash 写失败仅记录日志, 不影响 Stream 已写入的任务
        logger.warning("Hash 标记写入失败: task_id=%s error=%s", task_id, e)
    finally:
        hash_client.close()

    return task_id


class MqProducer:
    """发送识别结果到 Redis Stream"""

    def __init__(self, cfg: OmrConfig, client: Optional[MqClient] = None):
        self.cfg = cfg
        self._client = client or MqClient(cfg)
        self._owned_client = client is None

    def connect(self):
        if self._owned_client:
            self._client.connect()
        return self

    def send_result(self, stream: Optional[str] = None, payload: Any = None, message_id: str = "*"):
        """发送 JSON 结果到 Redis Stream"""
        stream = stream or self.cfg.redis_result_stream
        return self._xadd(stream, payload, message_id)

    def send_job(self, payload: Any = None, message_id: str = "*"):
        """发送（重试）任务到 Redis 任务流"""
        return self._xadd(self.cfg.redis_job_stream, payload, message_id)

    def _xadd(self, stream: str, payload: Any, message_id: str = "*"):
        body = json.dumps(payload, ensure_ascii=False, default=str)
        try:
            msg_id = self._client.redis.xadd(stream, {"payload": body}, id=message_id)
            logger.debug("Redis 消息已发送: stream=%s id=%s", stream, msg_id)
            return msg_id
        except Exception as e:
            logger.error("Redis 消息发送失败: stream=%s error=%s", stream, e)
            raise

    def close(self):
        if self._owned_client:
            self._client.close()
