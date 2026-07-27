"""临时诊断脚本：直接拉 Nacos 配置中心 omr-service.yaml 的当前内容。

用法：
    python -m omr_service.scripts.dump_nacos_config
"""
from __future__ import annotations

import json
import logging
import os
import sys

# 允许脚本既可作为 ``python -m omr_service.scripts.dump_nacos_config`` 运行，
# 也可作为 ``python omr_service/scripts/dump_nacos_config.py`` 运行。
if __package__ in (None, ""):
    _HERE = os.path.dirname(os.path.abspath(__file__))
    _SCREENIMG = os.path.dirname(os.path.dirname(os.path.dirname(_HERE)))
    if _SCREENIMG not in sys.path:
        sys.path.insert(0, _SCREENIMG)
    from omr_service.config import OmrConfig
    from omr_service.nacos_config import NacosConfigClient
else:
    from ..config import OmrConfig
    from ..nacos_config import NacosConfigClient


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    cfg = OmrConfig.from_env()
    print("=" * 72)
    print("OmrConfig（来自 .env / 环境变量 / Nacos 配置中心合并后的最终值）")
    print("=" * 72)
    print(f"  nacos_server             = {cfg.nacos_server}")
    print(f"  nacos_namespace          = {cfg.nacos_namespace}")
    print(f"  nacos_group_name         = {cfg.nacos_group_name}")
    print(f"  nacos_service_name       = {cfg.nacos_service_name}")
    print(f"  service_version          = {cfg.service_version!r}   ← 期望空串")
    print(f"  nacos_config_data_id     = {cfg.nacos_config_data_id}")
    print(f"  nacos_config_group       = {cfg.nacos_config_group}")
    print()

    # 直接拉 Nacos 配置中心的原始内容
    print("=" * 72)
    print(f"Nacos 配置中心原始内容：{cfg.nacos_config_data_id} @ group={cfg.nacos_config_group} namespace={cfg.nacos_namespace}")
    print("=" * 72)
    client = NacosConfigClient(cfg)
    try:
        content = client.load()
        print(content if isinstance(content, str) else json.dumps(content, ensure_ascii=False, indent=2))
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())