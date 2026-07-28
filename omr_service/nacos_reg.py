"""Nacos 应用级自注册（基于 nacos-sdk-python v2/v3 gRPC 协议）。"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Optional

from omr_service.config import OmrSettings
from omr_service.nacos_v2_compat import (
    import_client_config,
    import_deregister_instance_param,
    import_naming_service,
    import_register_instance_param,
    shared_loop,
)

logger = logging.getLogger(__name__)

_active_registrators: list["NacosRegistrator"] = []


class NacosRegistrator:
    """Nacos HTTP 应用实例注册器。"""

    def __init__(self, settings: OmrSettings):
        self.settings = settings
        self._naming_service: Optional = None
        self._loop = shared_loop()

    def build_metadata(self) -> dict:
        """仅应用级 metadata. 接口级注册 (providers:omr.OmrService::) 已删除."""
        return {
            "protocol": "http",
            "port": str(self.settings.http_port),
            "version": "2.0.0",
            "tag": os.getenv("OMR_TAG", ""),
            "health_check_url": f"http://{self.settings.nacos_ip}:{self.settings.http_port}/v1/health",
        }

    def _make_client_config(self):
        ClientConfig = import_client_config()
        return ClientConfig(
            server_addresses=self.settings.nacos_server,
            namespace_id=self.settings.nacos_namespace or "public",
            log_level=logging.INFO,
        )

    def _instance_ip(self) -> str:
        return self.settings.nacos_ip or "127.0.0.1"

    def register(self) -> bool:
        """向 Nacos 注册一个应用级 HTTP 实例。"""
        NacosNamingService = import_naming_service()
        client_config = self._make_client_config()

        async def _create():
            return await NacosNamingService.create_naming_service(client_config)

        try:
            self._naming_service = self._loop.run(_create())
        except Exception as exc:
            logger.error("Nacos NamingService 初始化失败: %s", exc)
            return False

        try:
            self.deregister()
        except Exception as exc:
            logger.warning("deregister 旧 instance 失败（可能没有旧 instance）: %s", exc)
        self._loop.run(asyncio.sleep(0.5))

        Param = import_register_instance_param()
        metadata = self.build_metadata()
        logger.debug(
            "DEBUG register %s metadata=%s",
            self.settings.nacos_service_name,
            json.dumps(metadata, ensure_ascii=False),
        )
        coro = self._naming_service.register_instance(
            Param(
                service_name=self.settings.nacos_service_name,
                group_name=self.settings.nacos_group,
                ip=self._instance_ip(),
                port=self.settings.http_port,
                metadata=metadata,
                healthy=True,
                enabled=True,
                weight=1.0,
                ephemeral=True,
            )
        )
        try:
            ok = self._loop.run(coro)
            if ok:
                logger.info(
                    "Nacos 注册成功: %s@%s:%s",
                    self.settings.nacos_service_name,
                    self._instance_ip(),
                    self.settings.http_port,
                )
                if self not in _active_registrators:
                    _active_registrators.append(self)
            else:
                logger.warning("Nacos 注册 %s 返回 False", self.settings.nacos_service_name)
            return ok
        except Exception as exc:
            logger.error("Nacos 注册 %s 失败: %s", self.settings.nacos_service_name, exc)
            return False

    def deregister(self) -> bool:
        """从 Nacos 注销应用级 HTTP 实例。"""
        if self._naming_service is None:
            return True

        Param = import_deregister_instance_param()
        coro = self._naming_service.deregister_instance(
            Param(
                service_name=self.settings.nacos_service_name,
                group_name=self.settings.nacos_group,
                ip=self._instance_ip(),
                port=self.settings.http_port,
                ephemeral=True,
            )
        )
        try:
            ok = self._loop.run(coro)
            if ok:
                logger.info("Nacos 注销成功: %s", self.settings.nacos_service_name)
            else:
                logger.warning("Nacos 注销 %s 返回 False", self.settings.nacos_service_name)
            return ok
        except Exception as exc:
            logger.error("Nacos 注销 %s 失败: %s", self.settings.nacos_service_name, exc)
            return False

    def close(self) -> None:
        """注销实例并释放资源。"""
        try:
            self.deregister()
        finally:
            if self._naming_service is not None:
                try:
                    self._loop.run(self._naming_service.shutdown())
                except Exception as exc:
                    logger.warning("Nacos NamingService 关闭异常: %s", exc)
                self._naming_service = None
            if self in _active_registrators:
                _active_registrators.remove(self)


def deregister_all() -> None:
    """注销当前进程创建的全部 Nacos 注册器。"""
    for registrator in list(_active_registrators):
        registrator.close()
