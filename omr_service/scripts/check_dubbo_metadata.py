"""Dubbo 3 接口级服务发现 —— omr-service Provider metadata 校验脚本。

目的
----
诊断 ``No provider available from registry ... Directory(invokers: 0[])`` 这类问题。

Dubbo 3 接口级服务发现（migration: FORCE_INTERFACE）会在 consumer 拿到
Nacos 上的 provider 实例列表后，再按 metadata 的以下字段做严格过滤：

  - interface   ：Java 端 ``@DubboReference(interfaceClass=...)`` 的 canonical name
  - methods     ：Java 端 stub 接口上声明的方法列表（camelCase、顺序敏感、大小写敏感）
  - version     ：``@DubboReference`` 默认空串，与 provider metadata 必须一致
  - group       ：``@DubboReference(group=...)`` 与 provider metadata 必须一致
  - protocol    ：``@DubboReference(protocol=...)`` 必须匹配（"tri"）
  - tri.service ：Triple 协议下 service key
  - dubbo.endpoints[].port ：Triple 端口

任意一项不匹配 → 整个 provider 被 InterfaceRouter 过滤 → ``invokers: 0``。

使用
----
::

    # 默认从 screenImg/.env 读 Nacos 配置
    python -m omr_service.scripts.check_dubbo_metadata

    # 自定义环境
    NACOS_SERVER=10.0.0.1:8848 \\
    NACOS_NAMESPACE=public \\
    NACOS_USERNAME=nacos NACOS_PASSWORD=*** \\
    OMR_DUBBO_PORT=20884 \\
    python -m omr_service.scripts.check_dubbo_metadata

退出码
----
- 0：所有关键字段对齐，gRPC 端口可达
- 1：存在不匹配或端口不可达（详见输出）
- 2：Nacos 查询失败
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import socket
import sys
import tempfile
from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Optional, Tuple

# 允许脚本既可作为 ``python -m omr_service.scripts.check_dubbo_metadata`` 运行，
# 也可作为 ``python omr_service/scripts/check_dubbo_metadata.py`` 运行。
if __package__ in (None, ""):
    # 目录直接执行模式：把 screenImg 加进 sys.path
    _HERE = os.path.dirname(os.path.abspath(__file__))
    _SCREENIMG = os.path.dirname(os.path.dirname(os.path.dirname(_HERE)))
    if _SCREENIMG not in sys.path:
        sys.path.insert(0, _SCREENIMG)
    from omr_service.config import OmrConfig
    from omr_service.nacos_v2_compat import (
        import_client_config,
        import_list_instance_param,
        import_naming_service,
        shared_loop,
    )
else:
    from ..config import OmrConfig
    from ..nacos_v2_compat import (
        import_client_config,
        import_list_instance_param,
        import_naming_service,
        shared_loop,
    )


logger = logging.getLogger("check_dubbo_metadata")

# Java 端 ``@DubboReference(interfaceClass = OmrService.class, protocol = "tri", group = "")``
# 注入时 Dubbo 3 期望的 provider metadata 形态。
EXPECTED = {
    "interface": "omr.OmrService",
    "methods": [
        "parseGoldenTemplate",
        "recognizeByTemplate",
        "verifyRecognitionRate",
        "reverifyPaper",
    ],
    "group": "",
    "version": "",
    "protocol": "tri",
    "tri.service": "omr.OmrService",
}

INTERFACE_SERVICE_NAME = "providers:omr.OmrService::"
APP_SERVICE_NAME = "omr-service"


# ---------------------------------------------------------------------------
# 报告模型
# ---------------------------------------------------------------------------


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str
    expected: Any = None
    actual: Any = None


@dataclass
class Report:
    results: List[CheckResult] = field(default_factory=list)
    instances: List[Dict[str, Any]] = field(default_factory=list)
    endpoint_checked: Optional[Tuple[str, int, bool]] = None

    def add(self, r: CheckResult) -> None:
        self.results.append(r)

    @property
    def failed(self) -> List[CheckResult]:
        return [r for r in self.results if not r.ok]

    @property
    def passed(self) -> bool:
        return not self.failed


# ---------------------------------------------------------------------------
# Nacos 查询
# ---------------------------------------------------------------------------


def _apply_windows_cache_patch() -> None:
    """Windows 上 monkey-patch SDK 磁盘缓存写入，避免 serviceName 里的 ':' 触发 OSError。

    本脚本作为 __main__ 运行时执行一次即可；被当作模块导入时不应修改全局状态。
    """
    try:
        from v2.nacos.utils import file_util as _file_util  # type: ignore

        async def _noop_write_to_file(logger, file_path, content):
            return None

        _file_util.write_to_file = _noop_write_to_file  # type: ignore
        # ServiceInfoCache 模块里 import 时把这个名字记到自己作用域了，
        # 也得同步覆盖一份，否则它仍会调用旧函数
        try:
            from v2.nacos.naming.cache import service_info_cache as _sic  # type: ignore

            _sic.write_to_file = _noop_write_to_file  # type: ignore
        except Exception:
            pass
    except Exception:
        pass


def _query_nacos(cfg: OmrConfig, service_name: str, group_name: str) -> List[Dict[str, Any]]:
    """查询 Nacos 上指定 serviceName + groupName 下的实例列表（gRPC 协议）。

    注意：在 Windows 上 nacos-sdk-python 的本地磁盘缓存路径会被 serviceName 里
    的 ':' 字符误识别为驱动器号分隔符（NTFS 把 ':' 当盘符），触发
    ``OSError(22, 'Invalid argument')``。本脚本做两件事绕开它：

    1. 显式把 SDK 的 cache_dir 设为短路径，禁用 load_cache_at_start
    2. monkey-patch ``v2.nacos.utils.file_util.write_to_file`` 让缓存写入静默失败，
       避免 Nacos 服务推送 service 信息时 SDK 尝试写本地文件触发同一个错误
       （仅当脚本作为 __main__ 运行时启用）。
    """
    ClientConfig = import_client_config()
    NacosNamingService = import_naming_service()
    ListInstanceParam = import_list_instance_param()
    loop = shared_loop()

    cache_dir = os.path.join(tempfile.gettempdir(), "nacos_check_meta")
    os.makedirs(cache_dir, exist_ok=True)

    client_config = ClientConfig(
        server_addresses=cfg.nacos_server,
        namespace_id=cfg.nacos_namespace or "public",
        username=cfg.nacos_username or "",
        password=cfg.nacos_password or "",
        log_level=logging.WARNING,
    )
    if hasattr(client_config, "set_cache_dir"):
        client_config.set_cache_dir(cache_dir)
    if hasattr(client_config, "set_load_cache_at_start"):
        client_config.set_load_cache_at_start(False)
    if hasattr(client_config, "set_update_cache_when_empty"):
        client_config.set_update_cache_when_empty(True)

    async def _list():
        svc = await NacosNamingService.create_naming_service(client_config)
        try:
            # 直接调 grpc_client_proxy.query_instance_of_service 走 Nacos 服务端
            # 实时查询（绕过 SDK service_info_holder 的内存缓存）。
            # 用法：query_instance_of_service(service_name, group_name, clusters, health_only)
            proxy = getattr(svc, "grpc_client_proxy", None)
            if proxy is None:
                raise RuntimeError("无法访问 grpc_client_proxy（SDK 内部结构变更？）")
            info = await proxy.query_instance_of_service(
                service_name=service_name,
                group_name=group_name,
                clusters="",
                health_only=False,
            )
            return list(getattr(info, "hosts", []) or []) if info is not None else []
        finally:
            try:
                await svc.shutdown()
            except Exception:
                pass

    return loop.run(_list())


def _extract_metadata(instance: Any) -> Dict[str, str]:
    """从 Nacos 实例对象中提取 metadata 字典（兼容 dict / 对象两种返回）。"""
    if isinstance(instance, dict):
        md = instance.get("metadata") or {}
    else:
        md = getattr(instance, "metadata", None) or {}
    # nacos-sdk-python 可能把 metadata 放在 instance.metadata 是 dict，也可能是 None
    if md is None:
        return {}
    # metadata value 一律转字符串，对照 Nacos 控制台显示
    return {str(k): ("" if v is None else str(v)) for k, v in md.items()}


def _extract_endpoint(instance: Any) -> Tuple[str, int]:
    if isinstance(instance, dict):
        return str(instance.get("ip") or instance.get("ipAddr") or ""), int(instance.get("port") or 0)
    return str(getattr(instance, "ip", "") or ""), int(getattr(instance, "port", 0) or 0)


# ---------------------------------------------------------------------------
# 检查项
# ---------------------------------------------------------------------------


def _check_metadata_fields(
    metadata: Dict[str, str],
    expected_group: str,
    expected_version: str,
) -> Report:
    r = Report()

    # 1. interface
    r.add(CheckResult(
        name="metadata.interface",
        ok=metadata.get("interface") == EXPECTED["interface"],
        detail="Java 端 @DubboReference(interfaceClass=OmrService.class) 期望的 canonical name",
        expected=EXPECTED["interface"],
        actual=metadata.get("interface", "<missing>"),
    ))

    # 2. methods —— 严格按顺序、大小写敏感比对
    actual_methods_str = metadata.get("methods", "")
    actual_methods = [m for m in actual_methods_str.split(",") if m]
    r.add(CheckResult(
        name="metadata.methods",
        ok=actual_methods == EXPECTED["methods"],
        detail="Dubbo 3 接口级订阅对 methods 字段大小写敏感、顺序敏感精确比对",
        expected=EXPECTED["methods"],
        actual=actual_methods,
    ))

    # 3. version —— 跟 EXPECTED["version"]（来自 Python 端 service_version）
    r.add(CheckResult(
        name="metadata.version",
        ok=metadata.get("version", "") == expected_version,
        detail=(
            "version 必须为空串（与 @DubboReference 默认 version='' 对齐）。"
            "非空值会让 InterfaceRouter 按非空严格匹配把所有 instance 过滤掉，"
            "表现就是 'No provider available, invokers: 0'。"
        ),
        expected=expected_version,
        actual=metadata.get("version", "<missing>"),
    ))

    # 4. group —— 跟 EXPECTED["group"] 和 nacos 注册组
    r.add(CheckResult(
        name="metadata.group",
        ok=metadata.get("group", "") == expected_group,
        detail="Java 端 @DubboReference(group='') 期望的 group 字段；与 Nacos 注册组是两件事，"
               "本项只检查 metadata 内的 group 字段。",
        expected=expected_group,
        actual=metadata.get("group", "<missing>"),
    ))

    # 5. protocol
    r.add(CheckResult(
        name="metadata.protocol",
        ok=metadata.get("protocol") == EXPECTED["protocol"],
        detail="Java 端 @DubboReference(protocol='tri') 期望值",
        expected=EXPECTED["protocol"],
        actual=metadata.get("protocol", "<missing>"),
    ))

    # 6. tri.service
    r.add(CheckResult(
        name="metadata.tri.service",
        ok=metadata.get("tri.service") == EXPECTED["tri.service"],
        detail="Triple 协议下 provider 在 Nacos 上声明的 service key",
        expected=EXPECTED["tri.service"],
        actual=metadata.get("tri.service", "<missing>"),
    ))

    # 7. dubbo.endpoints 里的 port
    endpoints_raw = metadata.get("dubbo.endpoints", "")
    try:
        endpoints = json.loads(endpoints_raw) if endpoints_raw else []
    except Exception:
        endpoints = []
    ports = [int(e.get("port", 0)) for e in endpoints if isinstance(e, dict)]
    r.add(CheckResult(
        name="metadata.dubbo.endpoints[].port",
        ok=len(ports) > 0,  # 端口非空即视为通过；具体端口可达性走 TCP 探测
        detail="dubbo.endpoints 必须至少包含一个 Triple 端口条目",
        expected="non-empty port list",
        actual=ports,
    ))

    return r


def _check_registration_group(cfg: OmrConfig, instances: List[Dict[str, Any]]) -> CheckResult:
    """检查 provider 实际注册到了 Python 端期望的 group。"""
    # nacos-sdk-python 的 host 对象没直接暴露 group；从查询参数推断。
    # 实际查到的实例若非空，说明 query group 是对的；若为空则需要切换 group 再查。
    return CheckResult(
        name=f"registration.group={cfg.nacos_group_name}",
        ok=len(instances) > 0,
        detail=(
            "Python 端 OmrConfig.nacos_group_name 必须与 Java 端 dubbo.registry.group 一致。"
            f"已在 group={cfg.nacos_group_name} 下查询 {INTERFACE_SERVICE_NAME}。"
        ),
        expected=f">=1 instance in group={cfg.nacos_group_name}",
        actual=f"{len(instances)} instance(s)",
    )


def _check_tcp_reachable(host: str, port: int, timeout: float = 3.0) -> Tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            # Triple 端口在收到任意 TCP 数据后会返回 HTTP/2 SETTINGS 帧。
            # 我们只验证 TCP 三次握手成功 + 端口没立刻 RST。
            sock.settimeout(timeout)
            try:
                sock.send(b"GET / HTTP/1.1\r\nHost: %s\r\n\r\n" % host.encode())
                peek = sock.recv(64)
                if peek:
                    # 截断展示
                    return True, f"TCP ok, recv {len(peek)} bytes (head={peek[:8]!r})"
            except socket.timeout:
                return True, "TCP ok, no immediate response (still listening)"
        return True, "TCP ok"
    except Exception as exc:
        return False, f"TCP failed: {exc}"


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------


def run(cfg: OmrConfig) -> Report:
    report = Report()

    # 1. 查 providers:omr.OmrService:: 在 nacos_group_name 下的实例
    logger.info(
        "查询 Nacos %s @ namespace=%s group=%s serviceName=%s",
        cfg.nacos_server, cfg.nacos_namespace, cfg.nacos_group_name, INTERFACE_SERVICE_NAME,
    )
    try:
        interface_instances = _query_nacos(cfg, INTERFACE_SERVICE_NAME, cfg.nacos_group_name)
    except Exception as exc:
        logger.error("查询接口级服务失败: %s", exc)
        return report  # 调用方会基于空结果判断失败

    report.instances = interface_instances

    # 2. 注册组对齐检查（基于查询结果）
    report.add(_check_registration_group(cfg, interface_instances))

    if not interface_instances:
        report.add(CheckResult(
            name="provider.instance.present",
            ok=False,
            detail=f"Nacos 在 group={cfg.nacos_group_name} 下找不到 {INTERFACE_SERVICE_NAME} 的实例。"
                   "Python 端 nacos_group_name 与 Java 端 dubbo.registry.group 必须一致。",
        ))
        return report

    # 3. 逐个实例检查 metadata
    for idx, inst in enumerate(interface_instances):
        ip, port = _extract_endpoint(inst)
        logger.info("实例 #%d: ip=%s port=%d", idx, ip, port)
        meta = _extract_metadata(inst)
        sub = _check_metadata_fields(
            meta,
            expected_group=EXPECTED["group"],
            expected_version=cfg.service_version,
        )
        # 给每个 check 名称加上实例索引
        for r in sub.results:
            r.name = f"#{idx} {r.name}"
        report.results.extend(sub.results)

        # 4. Triple 端口 TCP 可达性
        ok, detail = _check_tcp_reachable(ip, port)
        report.endpoint_checked = (ip, port, ok)
        report.add(CheckResult(
            name=f"#{idx} tcp:{ip}:{port}",
            ok=ok,
            detail="Dubbo Triple = HTTP/2 + gRPC，consumer 必须能 TCP 直连 provider 的这个端口",
            expected="TCP connectable",
            actual=detail,
        ))

    return report


def _render(report: Report, cfg: OmrConfig) -> str:
    lines: List[str] = []
    lines.append("=" * 72)
    lines.append("OMR Dubbo Provider Metadata 校验")
    lines.append("=" * 72)
    lines.append(f"Nacos     : {cfg.nacos_server}  namespace={cfg.nacos_namespace}")
    lines.append(f"Group     : {cfg.nacos_group_name}  (期望与 Java 端 dubbo.registry.group 一致)")
    lines.append(f"Service   : {INTERFACE_SERVICE_NAME}  /  {APP_SERVICE_NAME}")
    lines.append(f"Tri port  : {cfg.dubbo_port}")
    lines.append(f"version   : '{cfg.service_version}'  (期望为空串，与 @DubboReference 默认 version='' 对齐)")
    lines.append("")

    if not report.instances and not report.results:
        lines.append("⚠️  无法查询 Nacos 实例（详见上方日志）。")
        return "\n".join(lines)

    lines.append(f"实例数：{len(report.instances)}")
    for i, inst in enumerate(report.instances):
        ip, port = _extract_endpoint(inst)
        meta = _extract_metadata(inst)
        lines.append(f"  [{i}] ip={ip} port={port} healthy={getattr(inst, 'healthy', '?') if not isinstance(inst, dict) else inst.get('healthy', '?')}")
        for k in (
            "interface", "methods", "version", "revision", "group", "protocol",
            "tri.service", "dubbo.endpoints", "application",
            "side", "release", "meta-v", "tag", "dubbo.tag",
        ):
            if k in meta:
                v = meta[k]
                if len(v) > 80:
                    v = v[:77] + "..."
                lines.append(f"      {k} = {v}")
    lines.append("")

    lines.append("-" * 72)
    lines.append("检查项")
    lines.append("-" * 72)
    for r in report.results:
        marker = "✅" if r.ok else "❌"
        lines.append(f"{marker} {r.name}")
        if not r.ok:
            lines.append(f"     期望: {r.expected!r}")
            lines.append(f"     实际: {r.actual!r}")
            lines.append(f"     说明: {r.detail}")
    lines.append("")

    if report.passed:
        lines.append("🎉 所有检查通过。可以放心调用 omrService.* 接口。")
    else:
        lines.append(f"🚨 {len(report.failed)} 项不匹配，重点排查：")
        # 按失败影响程度排序提示
        severity = {
            "metadata.version": "version 不匹配是最常见的 'No provider' 根因",
            "metadata.interface": "interface 不匹配会让 InterfaceRouter 过滤掉整个实例",
            "metadata.methods": "methods 列表不匹配（顺序或大小写错误）",
            "metadata.protocol": "protocol 不为 tri 时 consumer 找不到 Triple 通道",
            "metadata.tri.service": "tri.service 不匹配会让 Triple 路由失败",
            "metadata.group": "metadata 内的 group 不为空会让 group 过滤跳过空组实例",
            "registration.group": "Nacos 注册组本身就跟 Java 端 consumer group 不一致",
            "tcp": "provider 端口不可达，consumer 拿到实例也连不上",
        }
        for r in report.failed:
            base_name = r.name.split(" ", 1)[-1]
            for key, hint in severity.items():
                if base_name.startswith(key) or key in base_name:
                    lines.append(f"   • [{key}] {hint}")
                    break
    lines.append("=" * 72)
    return "\n".join(lines)


def _render_group(group_name: str, sub: Report) -> List[str]:
    lines: List[str] = []
    lines.append("")
    lines.append("=" * 72)
    lines.append(f"扫描结果：group = {group_name}")
    lines.append("=" * 72)
    lines.append(f"实例数：{len(sub.instances)}")
    for i, inst in enumerate(sub.instances):
        ip, port = _extract_endpoint(inst)
        lines.append(f"  [{i}] ip={ip} port={port}")
        meta = _extract_metadata(inst)
        for k in ("interface", "methods", "version", "group", "protocol", "tri.service", "dubbo.endpoints"):
            if k in meta:
                v = meta[k]
                if len(v) > 80:
                    v = v[:77] + "..."
                lines.append(f"      {k} = {v}")
    lines.append("")
    for r in sub.results:
        marker = "✅" if r.ok else "❌"
        lines.append(f"{marker} {r.name}")
        if not r.ok:
            lines.append(f"     期望: {r.expected!r}")
            lines.append(f"     实际: {r.actual!r}")
    return lines


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="校验 omr-service 在 Nacos 上的 Dubbo provider metadata 是否与 Java 端 @DubboReference 兼容。",
    )
    parser.add_argument(
        "--nacos-server", default=None, help="覆盖 NACOS_SERVER，默认从 .env / 环境变量读"
    )
    parser.add_argument(
        "--nacos-namespace", default=None, help="覆盖 NACOS_NAMESPACE"
    )
    parser.add_argument(
        "--nacos-group", default=None, help="覆盖 NACOS_GROUP_NAME（Python 端注册组）"
    )
    parser.add_argument(
        "--service-version", default=None, help="覆盖期望的 service_version（应为空串）"
    )
    parser.add_argument(
        "--scan-all-groups",
        action="store_true",
        help="在 DEFAULT_GROUP 与 DUBBO_GROUP 两个常见 group 下都查一遍，"
             "用于快速判断 omr-service 实际注册到了哪个 group（修复中常见疑问）。",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="打开 DEBUG 日志"
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    cfg = OmrConfig.from_env()

    # 命令行覆盖
    if args.nacos_server:
        cfg.nacos_server = args.nacos_server
    if args.nacos_namespace:
        cfg.nacos_namespace = args.nacos_namespace
    if args.nacos_group:
        cfg.nacos_group_name = args.nacos_group
    if args.service_version is not None:
        cfg.service_version = args.service_version

    report = run(cfg)

    # 扫描模式：在多个常见 group 下都查一次，仅用于诊断定位
    scan_reports: List[Tuple[str, Report]] = []
    if args.scan_all_groups:
        for grp in ("DEFAULT_GROUP", "DUBBO_GROUP"):
            if grp == cfg.nacos_group_name:
                # 主流程已经查过了
                continue
            try:
                insts = _query_nacos(cfg, INTERFACE_SERVICE_NAME, grp)
            except OSError as exc:
                # nacos-sdk-python 在 Windows 上会用 groupName 作为本地缓存目录名，
                # 含 "DUBBO_GROUP" 这种字符的文件名是合法的，但放在路径里偶尔会触发
                # OSError(22, 'Invalid argument')，通常是 SDK 内部缓存路径问题。
                logger.warning("扫描 group=%s 失败（可能是 nacos-sdk-python 在本机的本地缓存问题，"
                               "不影响 Nacos 上的实例存在性）: %s", grp, exc)
                sub = Report(instances=[])
                sub.add(CheckResult(
                    name=f"registration.group={grp}",
                    ok=False,
                    detail=f"本地 SDK 缓存报错：{exc}。请用 Nacos 控制台或网页 UI 直接确认。",
                    expected=">=1 instance",
                    actual="N/A (SDK local cache failure)",
                ))
                scan_reports.append((grp, sub))
                continue
            except Exception as exc:
                logger.warning("扫描 group=%s 失败: %s", grp, exc)
                continue
            sub = Report(instances=insts)
            sub.add(_check_registration_group(
                type(cfg)(**{**cfg.__dict__, "nacos_group_name": grp}),
                insts,
            ))
            for idx, inst in enumerate(insts):
                ip, port = _extract_endpoint(inst)
                meta = _extract_metadata(inst)
                sub_meta = _check_metadata_fields(
                    meta,
                    expected_group=EXPECTED["group"],
                    expected_version=cfg.service_version,
                )
                for r in sub_meta.results:
                    r.name = f"#{idx} {r.name}"
                sub.results.extend(sub_meta.results)
                ok, detail = _check_tcp_reachable(ip, port)
                sub.endpoint_checked = (ip, port, ok)
                sub.add(CheckResult(
                    name=f"#{idx} tcp:{ip}:{port}",
                    ok=ok,
                    detail="Triple 端口 TCP 可达性",
                    expected="TCP connectable",
                    actual=detail,
                ))
            scan_reports.append((grp, sub))

    # 渲染主报告
    out = _render(report, cfg)
    print(out)

    # 渲染扫描结果
    for grp, sub in scan_reports:
        for line in _render_group(grp, sub):
            print(line)

    if not report.instances:
        return 2
    return 0 if report.passed else 1


if __name__ == "__main__":
    _apply_windows_cache_patch()
    sys.exit(main())