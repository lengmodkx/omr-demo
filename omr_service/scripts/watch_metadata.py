"""Nacos metadata 对账 watchdog。

定期检查 ``providers:omr.OmrService::`` 在 Nacos 上的 metadata 是否与
``OmrConfig.service_version`` 一致；若不一致则强制 deregister + register 一次。

用途
----
nacos-sdk-python v2/v3 的 redo_service 后台 redo 可能会用历史内存 metadata
覆盖新发起的 register_instance，导致 ``version`` 等字段回退到 ``1.0.0``，
从而让 Dubbo 3 接口级服务发现把 provider 全部过滤掉。本脚本作为兜底：

  - 启动后后台线程每 30s 检查一次
  - 若 metadata.version 与期望值不一致，调 deregister + register 强制对齐

用法
----
::

    python -m omr_service.scripts.watch_metadata

    python -m omr_service.scripts.watch_metadata --interval 60 --nacos-group DUBBO_GROUP
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

if __package__ in (None, ""):
    _HERE = os.path.dirname(os.path.abspath(__file__))
    _SCREENIMG = os.path.dirname(os.path.dirname(os.path.dirname(_HERE)))
    if _SCREENIMG not in sys.path:
        sys.path.insert(0, _SCREENIMG)
    from omr_service.config import OmrConfig
    from omr_service.nacos_v2_compat import (
        import_client_config,
        import_deregister_instance_param,
        import_naming_service,
        import_register_instance_param,
        shared_loop,
    )
else:
    from ..config import OmrConfig
    from ..nacos_v2_compat import (
        import_client_config,
        import_deregister_instance_param,
        import_naming_service,
        import_register_instance_param,
        shared_loop,
    )


logger = logging.getLogger("watch_metadata")

INTERFACE_SERVICE_NAME = "providers:omr.OmrService::"


def _build_metadata(cfg: OmrConfig) -> Dict[str, str]:
    """与 omr_service.nacos_reg._build_metadata 保持一致（接口级那一支）。"""
    meta = {
        "side": "provider",
        "release": "3.0.0_py",
        "protocol": "tri",
        "application": cfg.nacos_service_name,
        "dubbo.endpoints": json.dumps([{"port": cfg.dubbo_port, "protocol": "tri"}]),
        "dubbo.metadata.storage-type": "local",
        "meta-v": "2.0.0",
        "tag": cfg.service_tag or "",
        "interface": "omr.OmrService",
        "version": cfg.service_version,
        "group": "",
        "methods": "parseGoldenTemplate,recognizeByTemplate,verifyRecognitionRate,reverifyPaper",
        "generic": "false",
        "revision": cfg.service_version,
        "tri.service": "omr.OmrService",
    }
    return meta


def _force_reregister(cfg: OmrConfig, host: str, port: int) -> bool:
    """强制 deregister + register，使 Nacos 上的 metadata 与本地 cfg 对齐。"""
    ClientConfig = import_client_config()
    NacosNamingService = import_naming_service()
    RegParam = import_register_instance_param()
    DeRegParam = import_deregister_instance_param()
    loop = shared_loop()
    cc = ClientConfig(
        server_addresses=cfg.nacos_server,
        namespace_id=cfg.nacos_namespace or "public",
        username=cfg.nacos_username or "",
        password=cfg.nacos_password or "",
        log_level=logging.WARNING,
    )
    meta = _build_metadata(cfg)
    ok_holder: Dict[str, bool] = {}

    async def _do():
        svc = await NacosNamingService.create_naming_service(cc)
        try:
            await svc.deregister_instance(DeRegParam(
                service_name=INTERFACE_SERVICE_NAME,
                group_name=cfg.nacos_group_name,
                ip=host,
                port=port,
                ephemeral=True,
            ))
            ok_holder["dereg"] = True
            await svc.register_instance(RegParam(
                service_name=INTERFACE_SERVICE_NAME,
                group_name=cfg.nacos_group_name,
                ip=cfg.local_ip,
                port=cfg.dubbo_port,
                metadata=meta,
                healthy=True,
                enabled=True,
                weight=1.0,
                ephemeral=True,
            ))
            ok_holder["reg"] = True
        finally:
            await svc.shutdown()

    try:
        loop.run(_do())
        logger.warning(
            "FORCED REREGISTER ok: %s@%s:%s version=%r",
            INTERFACE_SERVICE_NAME, cfg.local_ip, cfg.dubbo_port, cfg.service_version,
        )
        return True
    except Exception as exc:
        logger.error("FORCED REREGISTER 失败: %s", exc)
        return False


def _check_once(cfg: OmrConfig) -> Tuple[bool, List[Dict[str, Any]]]:
    """检查 Nacos 上 metadata 是否与 cfg 对齐。返回 (ok, hosts)。"""
    ClientConfig = import_client_config()
    NacosNamingService = import_naming_service()
    loop = shared_loop()
    cc = ClientConfig(
        server_addresses=cfg.nacos_server,
        namespace_id=cfg.nacos_namespace or "public",
        username=cfg.nacos_username or "",
        password=cfg.nacos_password or "",
        log_level=logging.WARNING,
    )

    async def _query():
        svc = await NacosNamingService.create_naming_service(cc)
        try:
            info = await svc.grpc_client_proxy.query_instance_of_service(
                service_name=INTERFACE_SERVICE_NAME,
                group_name=cfg.nacos_group_name,
                clusters="",
                health_only=False,
            )
            return list(getattr(info, "hosts", []) or []) if info else []
        finally:
            await svc.shutdown()

    hosts = loop.run(_query())
    if not hosts:
        return False, []

    expected_version = cfg.service_version
    expected_interface = "omr.OmrService"
    for h in hosts:
        md = h.metadata or {}
        if md.get("version") != expected_version or md.get("interface") != expected_interface:
            return False, hosts
    return True, hosts


def _run_loop(cfg: OmrConfig, interval: int, stop_event: threading.Event) -> None:
    logger.info(
        "watch_metadata started: interval=%ds group=%s expected_version=%r",
        interval, cfg.nacos_group_name, cfg.service_version,
    )
    while not stop_event.is_set():
        try:
            ok, hosts = _check_once(cfg)
            if not ok:
                if not hosts:
                    logger.warning("Nacos 上找不到任何实例（%s @ %s）", INTERFACE_SERVICE_NAME, cfg.nacos_group_name)
                else:
                    for h in hosts:
                        md = h.metadata or {}
                        logger.warning(
                            "metadata 不一致: ip=%s port=%s version=%r interface=%r",
                            h.ip, h.port, md.get("version"), md.get("interface"),
                        )
                    # 强制重注册第一个实例
                    h = hosts[0]
                    _force_reregister(cfg, h.ip, h.port)
            else:
                logger.debug("metadata 一致，无须操作")
        except Exception as exc:
            logger.error("check_once 失败: %s", exc)

        # 可中断的 sleep
        stop_event.wait(interval)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Nacos metadata 对账 watchdog")
    parser.add_argument("--interval", type=int, default=30, help="检查间隔秒数（默认 30）")
    parser.add_argument("--nacos-group", default=None, help="覆盖 nacos_group_name")
    parser.add_argument("--once", action="store_true", help="只跑一次后退出")
    parser.add_argument("-v", "--verbose", action="store_true", help="打开 DEBUG 日志")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    cfg = OmrConfig.from_env()
    if args.nacos_group:
        cfg.nacos_group_name = args.nacos_group

    if args.once:
        ok, _ = _check_once(cfg)
        print(f"check_once result: {'OK' if ok else 'MISMATCH'}")
        return 0 if ok else 1

    stop_event = threading.Event()
    try:
        _run_loop(cfg, args.interval, stop_event)
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，退出")
        stop_event.set()
    return 0


if __name__ == "__main__":
    sys.exit(main())