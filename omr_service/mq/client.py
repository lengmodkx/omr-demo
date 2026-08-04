"""Redis 连接管理"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

import redis

from omr_service.config import OmrConfig

if TYPE_CHECKING:
    from omr_service.config import OmrSettings

logger = logging.getLogger(__name__)


def get_redis_client(settings: "OmrSettings | None" = None) -> redis.Redis:
    """返回已连接的 Redis 客户端.

    Args:
        settings: OmrSettings 实例；为 None 时回退到 OmrConfig.from_env().
    """
    if settings is not None:
        client = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            password=settings.redis_password or None,
            db=settings.redis_db,
            decode_responses=True,
            socket_connect_timeout=10,
            socket_timeout=10,
        )
    else:
        cfg = OmrConfig.from_env()
        client = redis.Redis(
            host=cfg.redis_host,
            port=cfg.redis_port,
            password=cfg.redis_password,
            db=cfg.redis_db,
            decode_responses=True,
            socket_connect_timeout=cfg.redis_timeout,
            socket_timeout=cfg.redis_timeout,
            ssl=cfg.redis_ssl,
        )
    client.ping()
    logger.info("Redis 连接成功: %s:%s/%s",
                client.connection_pool.connection_kwargs.get("host"),
                client.connection_pool.connection_kwargs.get("port"),
                client.connection_pool.connection_kwargs.get("db"))
    return client


class MqClient:
    """Redis 连接封装"""

    def __init__(self, cfg: OmrConfig):
        self.cfg = cfg
        self._client: Optional[redis.Redis] = None

    def connect(self) -> "MqClient":
        self._client = redis.Redis(
            host=self.cfg.redis_host,
            port=self.cfg.redis_port,
            password=self.cfg.redis_password,
            db=self.cfg.redis_db,
            decode_responses=True,
            socket_connect_timeout=self.cfg.redis_timeout,
            socket_timeout=self.cfg.redis_timeout,
            ssl=self.cfg.redis_ssl,
        )
        # 测试连接
        self._client.ping()
        logger.info("Redis 连接成功: %s:%s/%s", self.cfg.redis_host, self.cfg.redis_port, self.cfg.redis_db)
        return self

    @property
    def redis(self) -> redis.Redis:
        if self._client is None:
            raise RuntimeError("Redis 未连接，请先调用 connect()")
        return self._client

    def close(self):
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                logger.warning("关闭 Redis 失败: %s", e)
