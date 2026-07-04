"""Nacos 自注册（基于 nacos-sdk-python v2/v3 gRPC 协议）。"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

from omr_service.config import OmrConfig
from omr_service.nacos_v2_compat import (
    import_client_config,
    import_deregister_instance_param,
    import_naming_service,
    import_register_instance_param,
    shared_loop,
)

logger = logging.getLogger(__name__)

INTERFACE_SERVICE_NAME = "providers:omr.OmrService::"


class NacosRegistrator:
    """Nacos 实例注册器（gRPC 协议）。

    同时注册：
    1. 应用级服务发现：serviceName = omr-service
    2. 接口级服务发现：serviceName = providers:omr.OmrService::

    Java 消费端推荐配置（interface 级）：
      dubbo.application.service-discovery.migration=FORCE_INTERFACE
      dubbo.consumer.protocol=tri
    """

    def __init__(self, cfg: OmrConfig):
        self.cfg = cfg
        self._naming_service: Optional = None
        self._loop = shared_loop()

    def _build_metadata(self, interface: bool = False) -> dict:
        """Dubbo Triple 需要的公共元数据。

        字段格式参考 RuoYi-Cloud-Plus 框架 dubbo 3.3.x 默认生成的 metadata：
        - category=providers / dynamic=true / service-name-mapping=true
          这几个字段是 Dubbo 3 接口级订阅兼容性过滤器必须的，缺失会导致
          consumer 端 urls to invokers error。
        - path=接口全限定名：Dubbo 3 接口级服务发现校验。
        - dubbo / logger / deprecated：Dubbo 协议元数据。
        - port：Dubbo 3 自动选可用端口（不是写死的 dubbo_port），
          写死会导致 consumer 端反序列化时认为端口不匹配而丢弃 provider。

        字段精简原则：只保留 Dubbo 3.3.6 InterfaceRouter 真正要求的字段，
        避免 Python 端特有的 metadata（如 revision/tri.service 等）触发
        consumer 端兼容性过滤器的额外检查导致 invoker 创建失败。
        """
        # 应用级 metadata（不带接口信息）
        meta = {
            "side": "provider",
            "release": "3.3.6",   # 与 Java 端 Dubbo 版本一致
            "protocol": "tri",
            "application": self.cfg.nacos_service_name,
            "dubbo": "2.0.2",
            "deprecated": "false",
            "dynamic": "true",
            "generic": "false",
            "logger": "slf4j",
            "category": "providers",
            "service-name-mapping": "true",
            # 服务实例 Tag，用于同注册中心下多开发者本地调试隔离
            "tag": self.cfg.service_tag or "",
        }
        # Dubbo TagRouter 原生使用 dubbo.tag 作为路由键
        if self.cfg.service_tag:
            meta["dubbo.tag"] = self.cfg.service_tag
        if interface:
            # 接口级 metadata（必须在 application 级基础上加这些字段）
            meta.update(
                {
                    # ⚠️ 必须用 camelCase（对齐 Java 接口生成的方法名），
                    # 而非 .proto 里的 PascalCase。
                    # Dubbo 3 接口级订阅的兼容性过滤器是大小写敏感精确比对，
                    # PascalCase 会让所有 provider 被过滤掉 → "No provider available, invokers: 0"。
                    "interface": "omr.OmrService",
                    "path": "omr.OmrService",
                    "version": self.cfg.service_version,
                    "group": "",
                    "methods": "parseGoldenTemplate,recognizeByTemplate,verifyRecognitionRate,reverifyPaper",
                    # metadata-type=local 让 Dubbo 3 接口级服务发现用本地 metadata 而不是远程元数据中心
                    "metadata-type": "local",
                    # prefer.serialization 让 consumer 知道 provider 用什么序列化
                    "prefer.serialization": "hessian2,fastjson2",
                }
            )
        return meta

    def _make_client_config(self):
        ClientConfig = import_client_config()
        return ClientConfig(
            server_addresses=self.cfg.nacos_server,
            namespace_id=self.cfg.nacos_namespace or "public",
            username=self.cfg.nacos_username or "",
            password=self.cfg.nacos_password or "",
            log_level=logging.INFO,
        )

    def _register_one(self, service_name: str) -> bool:
        Param = import_register_instance_param()
        metadata = self._build_metadata(interface=service_name.startswith("providers:"))
        logger.debug("DEBUG register %s metadata=%s", service_name, json.dumps(metadata, ensure_ascii=False))
        coro = self._naming_service.register_instance(
            Param(
                service_name=service_name,
                group_name=self.cfg.nacos_group_name,
                ip=self.cfg.local_ip,
                port=self.cfg.dubbo_port,
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
                    "Nacos 注册成功: %s@%s:%s tag=%s",
                    service_name,
                    self.cfg.local_ip,
                    self.cfg.dubbo_port,
                    self.cfg.service_tag or "<baseline>",
                )
            else:
                logger.warning("Nacos 注册 %s 返回 False", service_name)
            return ok
        except Exception as exc:
            logger.error("Nacos 注册 %s 失败: %s", service_name, exc)
            return False

    def register(self) -> bool:
        """注册实例到 Nacos。"""
        NacosNamingService = import_naming_service()
        client_config = self._make_client_config()

        async def _create():
            return await NacosNamingService.create_naming_service(client_config)

        try:
            self._naming_service = self._loop.run(_create())
        except Exception as exc:
            logger.error("Nacos NamingService 初始化失败: %s", exc)
            return False

        # 先 deregister 同名旧 instance（Nacos SDK 的 register_instance 在已存在时
        # 只续约心跳不更新 metadata，必须先 deregister 才能让新 metadata 生效）。
        # 同时防止旧 OMR 进程崩溃后残留的 instance（ephemeral 没及时过期）。
        try:
            self.deregister()
        except Exception as exc:
            logger.warning("deregister 旧 instance 失败（可能没有旧 instance）: %s", exc)
        # 给 Nacos 服务端一点时间处理 deregister
        self._loop.run(asyncio.sleep(0.5))

        results = [
            self._register_one(self.cfg.nacos_service_name),
            self._register_one(INTERFACE_SERVICE_NAME),
        ]
        return any(results)

    def deregister(self) -> bool:
        """从 Nacos 注销实例。"""
        if self._naming_service is None:
            return True

        DeregisterInstanceParam = import_deregister_instance_param()
        ok_all = True
        for service_name in (self.cfg.nacos_service_name, INTERFACE_SERVICE_NAME):
            coro = self._naming_service.deregister_instance(
                DeregisterInstanceParam(
                    service_name=service_name,
                    group_name=self.cfg.nacos_group_name,
                    ip=self.cfg.local_ip,
                    port=self.cfg.dubbo_port,
                    ephemeral=True,
                )
            )
            try:
                ok = self._loop.run(coro)
                if ok:
                    logger.info("Nacos 注销成功: %s", service_name)
                else:
                    logger.warning("Nacos 注销 %s 返回 False", service_name)
                    ok_all = False
            except Exception as exc:
                logger.error("Nacos 注销 %s 失败: %s", service_name, exc)
                ok_all = False
        return ok_all

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
