"""检查所有 ruoyi 服务在 Nacos 上注册的 IP 是否统一（避免多网卡机器选错网卡）。

用法：
    python -m omr_service.scripts.check_service_ips
    python -m omr_service.scripts.check_service_ips --expected-ip 10.135.4.92
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from typing import List

if __package__ in (None, ""):
    _HERE = os.path.dirname(os.path.abspath(__file__))
    _SCREENIMG = os.path.dirname(os.path.dirname(os.path.dirname(_HERE)))
    if _SCREENIMG not in sys.path:
        sys.path.insert(0, _SCREENIMG)
    from omr_service.nacos_v2_compat import (
        import_client_config, import_naming_service, shared_loop,
    )
else:
    from ..nacos_v2_compat import (
        import_client_config, import_naming_service, shared_loop,
    )


# 期望检查的 ruoyi-* 服务列表（包含应用级 + 接口级两种 serviceName）
SERVICES = [
    ("ruoyi-gateway", "应用级"),
    ("ruoyi-auth", "应用级"),
    ("ruoyi-system", "应用级"),
    ("ruoyi-resource", "应用级"),
    ("ruoyi-gen", "应用级"),
    ("ruoyi-job", "应用级"),
    ("ruoyi-workflow", "应用级"),
    ("ruoyi-monitor", "应用级"),
    ("ruoyi-snailjob-server", "应用级"),
    ("ruoyi-exam-base", "应用级"),
    ("ruoyi-exam-admin", "应用级"),
    ("providers:org.dromara.system.api.RemoteLogService::", "Dubbo 接口级"),
    ("providers:org.dromara.system.api.RemoteTenantService::", "Dubbo 接口级"),
    ("providers:org.dromara.system.api.RemoteUserService::", "Dubbo 接口级"),
    ("providers:omr.OmrService::", "Dubbo 接口级"),
]


async def main_async(args) -> int:
    ClientConfig = import_client_config()
    NacosNamingService = import_naming_service()
    cc = ClientConfig(
        server_addresses=args.nacos_server,
        namespace_id=args.nacos_namespace,
        username=args.nacos_username or "",
        password=args.nacos_password or "",
        log_level=30,
    )
    svc = await NacosNamingService.create_naming_service(cc)
    try:
        print(f"== Nacos 服务 IP 一致性检查 ==")
        print(f"   server={args.nacos_server} namespace={args.nacos_namespace}")
        print(f"   expected IP = {args.expected_ip}")
        print(f"   groups = {args.groups}")
        print()

        all_ok = True
        bad_services: List[str] = []
        missing_services: List[str] = []

        for sn, kind in SERVICES:
            for grp in args.groups:
                try:
                    info = await asyncio.wait_for(
                        svc.grpc_client_proxy.query_instance_of_service(
                            service_name=sn, group_name=grp, clusters="", health_only=False,
                        ),
                        timeout=args.timeout,
                    )
                except Exception as exc:
                    print(f"  ⚠️ {sn} @ {grp} ({kind}): 查询失败 {exc}")
                    all_ok = False
                    bad_services.append(f"{sn}@{grp}: query failed")
                    continue
                hosts = info.hosts or []
                if not hosts:
                    missing_services.append(f"{sn}@{grp}")
                    print(f"  ⚪ {sn} @ {grp} ({kind}): 0 instance (服务未注册)")
                    continue
                for h in hosts:
                    actual_ip = h.ip
                    mark = "✅" if actual_ip == args.expected_ip else "❌"
                    if actual_ip != args.expected_ip:
                        bad_services.append(f"{sn}@{grp}: {actual_ip}")
                        all_ok = False
                    print(f"  {mark} {sn} @ {grp} ({kind}): ip={actual_ip} port={h.port}")
        print()
        if all_ok and not missing_services:
            print(f"🎉 所有服务 IP 均符合预期 ({args.expected_ip})")
            return 0
        if missing_services:
            print(f"⚠️  {len(missing_services)} 个 service 在 Nacos 上无任何 instance：")
            for s in missing_services:
                print(f"   - {s}")
            print()
        if bad_services:
            print(f"🚨 {len(bad_services)} 个服务 IP 不符合预期：")
            for s in bad_services:
                print(f"   - {s}")
            print()
        if not all_ok or missing_services:
            print("建议：")
            print("  1. 检查启动时是否设置了环境变量 DUBBO_HOST=" + args.expected_ip)
            print("  2. 或在 yml 里检查 dubbo.protocol.host 是否正确")
            print("  3. 重启有问题的服务（让 ruoyi-common-dubbo/common-dubbo.yml 生效）")
            print("  4. 检查服务进程是否真的启动了（netstat 看 9201/9210 等端口）")
            return 1
        return 0
    finally:
        await svc.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(description="检查 Nacos 上所有服务 IP 是否一致")
    parser.add_argument("--expected-ip", default=os.getenv("EXPECTED_IP", ""), help="期望的对外 IP")
    parser.add_argument("--nacos-server", default=os.getenv("NACOS_SERVER", ""))
    parser.add_argument("--nacos-namespace", default=os.getenv("NACOS_NAMESPACE", ""))
    parser.add_argument("--nacos-username", default=os.getenv("NACOS_USERNAME", ""))
    parser.add_argument("--nacos-password", default=os.getenv("NACOS_PASSWORD", ""))
    parser.add_argument("--timeout", type=float, default=10.0, help="单次 Nacos 查询超时（秒）")
    parser.add_argument(
        "--groups", nargs="+", default=["DEFAULT_GROUP", "DUBBO_GROUP"],
        help="要检查的 group 列表",
    )
    args = parser.parse_args()
    loop = shared_loop()
    return loop.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())