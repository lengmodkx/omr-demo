"""强制覆盖 Nacos 上 omr.OmrService 的 metadata。

背景：nacos-sdk-python v2/v3 的 redo_service 后台每 5s 重发一次 register_instance，
使用启动时内存里的旧 metadata（release=3.0.0_py、meta-v=2.0.0），导致即便我们手动
deregister + register，旧 metadata 也会被立刻覆盖回去。

用法：在新代码的 omr-service 进程启动之前，本脚本可以临时维持正确的 metadata。
或者杀掉旧进程后用本脚本 + 等待新进程启动前填补空窗期。

使用：
    python -m omr_service.scripts.force_fix_metadata
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

if __package__ in (None, ""):
    _HERE = os.path.dirname(os.path.abspath(__file__))
    _SCREENIMG = os.path.dirname(os.path.dirname(os.path.dirname(_HERE)))
    if _SCREENIMG not in sys.path:
        sys.path.insert(0, _SCREENIMG)
    from omr_service.nacos_v2_compat import (
        import_client_config,
        import_deregister_instance_param,
        import_naming_service,
        import_register_instance_param,
        shared_loop,
    )
else:
    from ..nacos_v2_compat import (
        import_client_config,
        import_deregister_instance_param,
        import_naming_service,
        import_register_instance_param,
        shared_loop,
    )


# 与 Java 端 RuoYi-Cloud-Plus dubbo 3.3.6 provider 完全对齐的 metadata
INTERFACE_METADATA = {
    "side": "provider",
    "release": "3.3.6",       # 与 Java 端 Dubbo 版本一致
    "protocol": "tri",
    "application": "omr-service",
    "dubbo": "2.0.2",
    "deprecated": "false",
    "dynamic": "true",
    "generic": "false",
    "logger": "slf4j",
    "category": "providers",
    "service-name-mapping": "true",
    "tag": "",
    "interface": "omr.OmrService",
    "path": "omr.OmrService",
    "version": "",
    "group": "",
    "methods": "parseGoldenTemplate,recognizeByTemplate,verifyRecognitionRate,reverifyPaper",
    "revision": "",
    "tri.service": "omr.OmrService",
    # 关键字段：Dubbo 3 接口级服务发现读这个短键名（之前用了 dubbo.metadata.storage-type 是错的）
    "metadata-type": "local",
    "prefer.serialization": "hessian2,fastjson2",
}

APP_METADATA = {
    "side": "provider",
    "release": "3.3.6",
    "protocol": "tri",
    "application": "omr-service",
    "dubbo": "2.0.2",
    "deprecated": "false",
    "dynamic": "true",
    "generic": "false",
    "logger": "slf4j",
    "category": "providers",
    "service-name-mapping": "true",
    "tag": "",
}


async def force_fix(cfg, host: str, port: int) -> bool:
    ClientConfig = import_client_config()
    NacosNamingService = import_naming_service()
    RegParam = import_register_instance_param()
    DeRegParam = import_deregister_instance_param()
    cc = ClientConfig(
        server_addresses=cfg.nacos_server,
        namespace_id=cfg.nacos_namespace or "public",
        username=cfg.nacos_username or "",
        password=cfg.nacos_password or "",
        log_level=30,
    )
    svc = await NacosNamingService.create_naming_service(cc)
    try:
        # 先清掉两个 serviceName 的旧实例
        for sn in ("providers:omr.OmrService::", "omr-service"):
            await svc.deregister_instance(DeRegParam(
                service_name=sn, group_name=cfg.nacos_group_name,
                ip=host, port=port, ephemeral=True,
            ))
        await asyncio.sleep(2)
        # 重新注册两个 serviceName（接口级 + 应用级）
        results = []
        for sn, md in (("providers:omr.OmrService::", INTERFACE_METADATA), ("omr-service", APP_METADATA)):
            ok = await svc.register_instance(RegParam(
                service_name=sn, group_name=cfg.nacos_group_name,
                ip=host, port=port, metadata=md,
                healthy=True, enabled=True, weight=1.0, ephemeral=True,
            ))
            results.append((sn, ok))
            print(f"register {sn}@{cfg.nacos_group_name} -> {ok}")
        return all(ok for _, ok in results)
    finally:
        await svc.shutdown()


async def force_fix_loop(cfg, host: str, port: int, interval: int, stop_event):
    print(f"=== force_fix_metadata loop start (interval={interval}s) ===")
    while not stop_event.is_set():
        try:
            ok = await force_fix(cfg, host, port)
            if ok:
                print(f"✅ metadata fixed at {asyncio.get_event_loop().time()}")
        except Exception as exc:
            print(f"⚠️  force_fix 失败: {exc}")
        stop_event.wait(interval)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, default=10, help="重写间隔秒数（默认 10）")
    parser.add_argument("--once", action="store_true", help="只跑一次后退出")
    parser.add_argument("--host", default="10.135.4.92", help="注册的 IP")
    parser.add_argument("--port", type=int, default=20884, help="注册的端口")
    parser.add_argument("--nacos-server", default="39.153.154.183:8848")
    parser.add_argument("--nacos-namespace", default="8c4541fd-870e-414d-bdee-72cab49fe8d2")
    parser.add_argument("--nacos-username", default="nacos")
    parser.add_argument("--nacos-password", default="lemon2judy")
    parser.add_argument("--nacos-group", default="DUBBO_GROUP")
    args = parser.parse_args()

    # 构造一个最小 cfg 对象
    @dataclass_like
    class _Cfg:
        pass

    cfg = _Cfg()
    cfg.nacos_server = args.nacos_server
    cfg.nacos_namespace = args.nacos_namespace
    cfg.nacos_username = args.nacos_username
    cfg.nacos_password = args.nacos_password
    cfg.nacos_group_name = args.nacos_group

    if args.once:
        loop = shared_loop()
        ok = loop.run(force_fix(cfg, args.host, args.port))
        return 0 if ok else 1
    else:
        import threading
        stop = threading.Event()
        try:
            loop = shared_loop()
            loop.run(force_fix_loop(cfg, args.host, args.port, args.interval, stop))
        except KeyboardInterrupt:
            print("\n收到 Ctrl+C，退出")
            stop.set()
        return 0


def dataclass_like(cls):
    """极简 dataclass 替代，避免 dataclass 导入。"""
    return cls


if __name__ == "__main__":
    sys.exit(main())