"""检查 Nacos 配置中心里某个 service 的所有 yaml 配置。"""
from __future__ import annotations

import argparse
import os
import sys

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-id", required=True)
    parser.add_argument("--group", default="DEFAULT_GROUP")
    args = parser.parse_args()

    cfg = OmrConfig.from_env()
    cfg_for_lookup = OmrConfig(
        nacos_server=cfg.nacos_server,
        nacos_namespace=cfg.nacos_namespace,
        nacos_username=cfg.nacos_username,
        nacos_password=cfg.nacos_password,
        nacos_config_data_id=args.data_id,
        nacos_config_group=args.group,
    )
    client2 = NacosConfigClient(cfg_for_lookup)
    try:
        content = client2.load()
        print(f"=== {args.data_id} @ group={args.group} ===")
        print(content if isinstance(content, str) else repr(content))
    finally:
        client.close()
        client2.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())