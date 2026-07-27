"""订阅 Nacos 上的 omr.OmrService 接口级 serviceName，看 push 给的 URL 是什么。

Dubbo 3.3.6 Java consumer 在 Nacos 上订阅 `providers:omr.OmrService::`，
然后用 Nacos 推过来的 URL 构造 invoker。如果 URL 不符合 consumer-side
expectation，就会被 InterfaceRouter 过滤掉，invokers=0。
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from omr_service.nacos_v2_compat import (
    import_client_config, import_naming_service, shared_loop,
    import_list_instance_param,
)


async def main():
    ClientConfig = import_client_config()
    NacosNamingService = import_naming_service()
    cc = ClientConfig(
        server_addresses="39.153.154.183:8848",
        namespace_id="8c4541fd-870e-414d-bdee-72cab49fe8d2",
        username="nacos", password="lemon2judy", log_level=30,
    )
    svc = await NacosNamingService.create_naming_service(cc)
    try:
        # 用 gRPC query 直接拉取（不走 subscribe 流程）
        info = await svc.grpc_client_proxy.query_instance_of_service(
            service_name="providers:omr.OmrService::",
            group_name="DUBBO_GROUP",
            clusters="",
            health_only=False,
        )
        hosts = info.hosts or [] if info else []


def flatten_print(host):
    pass


print(f"\n=== query_instance_of_service result: {len(hosts)} hosts ===")
        # 等 3 秒让 server push 数据进来
        await asyncio.sleep(3)
        # 查看 internal cache
        holder = getattr(svc, "service_info_holder", None)
        cache = (holder.service_info_map or {}) if holder else {}
        print(f"\n=== list_instances result: {len(hosts)} hosts ===")
        print("=== Internal cache: ===")
        for k, v in sorted(cache.items()):
            h = (getattr(v, "hosts", []) or [])
            print(f"\nKEY: {k!r}: {len(h)} hosts")
            for i, hh in enumerate(h):
                print(f"  host[{i}] ip={hh.ip} port={hh.port}")
                print(f"    enabled={hh.enabled} healthy={hh.healthy}")
                md = hh.metadata or {}
                if "tri.service" in md:
                    print("    tri.service ✓")
                if "interface" in md:
                    print(f"    interface = {md['interface']!r}")
                if "methods" in md:
                    methods_list = md["methods"].split(",")
                    print(f"    methods ({len(methods_list)}): {methods_list}")
    finally:
        await svc.shutdown()


loop = shared_loop()
loop.run(main())