#!/usr/bin/env python3
"""为 nacos-sdk-python 3.x 生成的 protobuf 文件打补丁。

nacos-sdk-python 3.2.0 生成的 gRPC pb 使用了 protobuf 5.x 的
`runtime_version` API，而 PaddleOCR/PaddlePaddle 稳定组合要求
protobuf<=3.20.2。本脚本在保持 nacos-sdk-python 功能的前提下，
移除对高版本 protobuf 运行时校验的依赖，使两个生态可以共存。

使用方法（仅在 .venv-py311 中运行一次即可）：
    python scripts/patch_nacos_protobuf.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path


def find_site_packages() -> Path | None:
    """定位当前虚拟环境的 site-packages 目录。"""
    for p in sys.path:
        if p.endswith("site-packages") or "site-packages" in p.replace("\\", "/"):
            path = Path(p)
            if path.is_dir():
                return path
    return None


def patch_file(path: Path) -> bool:
    """移除 _runtime_version 的 import 与校验调用。"""
    if not path.exists():
        return False
    original = path.read_text(encoding="utf-8")
    # 1) 删除 import runtime_version 行
    cleaned = re.sub(
        r"^from google\.protobuf import runtime_version as _runtime_version\n",
        "",
        original,
        flags=re.MULTILINE,
    )
    # 2) 删除 ValidateProtobufRuntimeVersion 调用块（含参数的多行）
    cleaned = re.sub(
        r"^_runtime_version\.ValidateProtobufRuntimeVersion\(\s*[^)]+\)\n",
        "",
        cleaned,
        flags=re.MULTILINE | re.DOTALL,
    )
    if cleaned == original:
        return False
    path.write_text(cleaned, encoding="utf-8")
    return True


def main() -> int:
    site_packages = find_site_packages()
    if site_packages is None:
        print("未找到 site-packages 目录，请确保在虚拟环境中运行。", file=sys.stderr)
        return 1

    targets = [
        site_packages / "v2" / "nacos" / "transport" / "grpcauto" / "nacos_grpc_service_pb2.py",
    ]

    patched = 0
    for target in targets:
        if patch_file(target):
            print(f"已补丁: {target}")
            patched += 1
        else:
            print(f"无需补丁或文件不存在: {target}")

    if patched == 0:
        print("没有文件被修改，可能已经打过补丁。", file=sys.stderr)
        return 0
    print(f"完成，共修改 {patched} 个文件。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
