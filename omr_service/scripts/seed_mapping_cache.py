"""Seed Dubbo interface-apps mapping cache for omr-service.

Dubbo 3.3.6 with migration: APPLICATION_FIRST requires each consumer to know
"interface name → application name" mapping. Java providers auto-expose
Dubbo MetadataService.getServiceMapping() to populate this cache.

Our Python omr-service uses nacos-sdk-python which doesn't expose Dubbo's
MetadataService, so the mapping is never discovered. We seed the cache
file directly so ServiceDiscoveryRegistryDirectory can find the
application to subscribe to.

Cache file location: C:\\Users\\<user>\\.dubbo\\.mapping.<appname>.dubbo.cache

Usage:
    python -m omr_service.scripts.seed_mapping_cache
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path


DEFAULT_MAPPINGS = {
    # Dubbo consumer application name: [(interface, app), ...]
    "ruoyi-exam-admin": [
        ("omr.OmrService", "omr-service"),
    ],
    # 加上以防未来其他 java 服务也调 omr-service
    "ruoyi-gateway": [
        ("omr.OmrService", "omr-service"),
    ],
    "ruoyi-auth": [
        ("omr.OmrService", "omr-service"),
    ],
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", default=None, help="Windows 用户名（默认用 %USERPROFILE% 环境变量）")
    args = parser.parse_args()

    user_dir = Path(args.user or os.environ.get("USERPROFILE", "."))
    dubbo_dir = user_dir / ".dubbo"
    if not dubbo_dir.exists():
        print(f"未找到 {dubbo_dir}，请先启动一次对应的 Java 服务让 Dubbo 创建缓存目录")
        return 1

    modified = []
    for app_name, mappings in DEFAULT_MAPPINGS.items():
        cache_file = dubbo_dir / f".mapping.{app_name}.dubbo.cache"
        if not cache_file.exists():
            print(f"  (跳过) {cache_file} 不存在")
            continue

        content = cache_file.read_text(encoding="utf-8")
        original_lines = content.splitlines()
        new_lines = list(original_lines)
        inserted_any = False
        for interface, app in mappings:
            line_to_add = f'{interface}=["{app}"]'
            if line_to_add in content:
                continue
            inserted = False
            for i, line in enumerate(new_lines):
                if line.startswith("org.dromara.") and interface < line:
                    new_lines.insert(i, line_to_add)
                    inserted = True
                    inserted_any = True
                    break
            if not inserted:
                new_lines.append(line_to_add)
                inserted_any = True

        if not inserted_any:
            print(f"  {app_name}: 已包含所有映射")
            continue

        new_content = "\n".join(new_lines) + "\n"
        cache_file.write_text(new_content, encoding="utf-8")
        modified.append(app_name)
        print(f"  ✓ {app_name}: 已写入映射")

    if not modified:
        print("所有 mapping cache 已经包含所需的映射")
        return 0
    print(f"\n已修改: {', '.join(modified)}")
    print("注意：下次启动对应 Java 服务时就会使用新映射加载")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())