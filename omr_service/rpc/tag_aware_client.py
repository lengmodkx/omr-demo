"""Tag 感知的 OmrService gRPC 示例客户端。

用法示例：
    export OMR_SERVICE_TAG=zhangsan
    export NACOS_SERVER=127.0.0.1:8848
    python -m omr_service.rpc.tag_aware_client --method RecognizeByTemplate --template-id 1 --image-url "http://..."

说明：
- 先从 Nacos 拉取 ``providers:omr.OmrService::`` 的实例列表。
- 按 ``metadata.tag`` 过滤，优先选择带相同 Tag 的实例。
- 若未命中，则 fallback 到 ``tag`` 为空/未设置的基线实例。
- 调用时会自动在 gRPC metadata 中携带 ``x-service-tag``，供 Provider 端验证。
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import sys
from typing import List, Optional

# 必须先加载 Nacos v2 兼容补丁，否则导入 v2.nacos 可能因 a2a 类型冲突而失败。
from omr_service.nacos_v2_compat import (
    import_client_config,
    import_list_instance_param,
    import_naming_service,
    shared_loop,
)

import grpc

from omr_service.config import OmrConfig
from omr_service.rpc import omr_pb2, omr_pb2_grpc

logger = logging.getLogger(__name__)

INTERFACE_SERVICE_NAME = "providers:omr.OmrService::"
TAG_HEADER = "x-service-tag"


def _pick_instance(instances: list, target_tag: str) -> Optional[object]:
    """按 target_tag 筛选实例，未命中则 fallback 到基线实例。"""
    if not instances:
        return None

    # 优先选择带相同 Tag 的实例
    tagged = [
        inst
        for inst in instances
        if (inst.metadata or {}).get("tag") == target_tag and target_tag
    ]
    if tagged:
        return random.choice(tagged)

    # fallback 到基线实例（tag 为空或未设置）
    baseline = [
        inst
        for inst in instances
        if not (inst.metadata or {}).get("tag")
    ]
    if baseline:
        return random.choice(baseline)

    # 若没有任何基线实例，则随机返回一个，避免完全不可用
    return random.choice(instances)


class TagAwareOmrClient:
    """通过 Nacos Tag 路由发现 OMR 服务实例并发起 gRPC 调用的示例客户端。"""

    def __init__(self, cfg: OmrConfig, target_tag: str = ""):
        self.cfg = cfg
        self.target_tag = target_tag or ""
        self._naming_service = None
        self._loop = shared_loop()

    def _make_client_config(self):
        ClientConfig = import_client_config()
        return ClientConfig(
            server_addresses=self.cfg.nacos_server,
            namespace_id=self.cfg.nacos_namespace or "public",
            username=self.cfg.nacos_username or "",
            password=self.cfg.nacos_password or "",
            log_level=logging.WARNING,
        )

    def _ensure_naming_service(self):
        if self._naming_service is not None:
            return
        NacosNamingService = import_naming_service()
        client_config = self._make_client_config()

        async def _create():
            return await NacosNamingService.create_naming_service(client_config)

        self._naming_service = self._loop.run(_create())

    def discover(self) -> Optional[str]:
        """返回选中的实例地址 ``ip:port``。"""
        self._ensure_naming_service()
        ListInstanceParam = import_list_instance_param()
        coro = self._naming_service.list_instances(
            ListInstanceParam(
                service_name=INTERFACE_SERVICE_NAME,
                group_name=self.cfg.nacos_group_name,
                subscribe=False,
                healthy_only=True,
            )
        )
        try:
            instances = self._loop.run(coro) or []
        except Exception as exc:
            logger.error("Nacos 查询实例失败: %s", exc)
            return None

        logger.info(
            "Nacos 返回 %d 个实例，目标 tag=%s",
            len(instances),
            self.target_tag or "<baseline>",
        )
        inst = _pick_instance(instances, self.target_tag)
        if inst is None:
            return None
        logger.info(
            "选中实例 %s:%s (tag=%s)",
            inst.ip,
            inst.port,
            (inst.metadata or {}).get("tag") or "<baseline>",
        )
        return f"{inst.ip}:{inst.port}"

    def call(self, method: str, request, timeout: float = 30.0):
        """发起一次 gRPC 调用，自动附加 x-service-tag metadata。"""
        address = self.discover()
        if not address:
            raise RuntimeError("未找到可用的 OMR 服务实例")

        metadata = []
        if self.target_tag:
            metadata.append((TAG_HEADER, self.target_tag))

        with grpc.insecure_channel(
            address,
            options=[
                ("grpc.max_send_message_length", 50 * 1024 * 1024),
                ("grpc.max_receive_message_length", 50 * 1024 * 1024),
            ],
        ) as channel:
            stub = omr_pb2_grpc.OmrServiceStub(channel)
            rpc = getattr(stub, method, None)
            if rpc is None:
                raise ValueError(f"不存在的方法: {method}")
            return rpc(request, timeout=timeout, metadata=metadata)

    def close(self):
        if self._naming_service is not None:
            try:
                self._loop.run(self._naming_service.shutdown())
            except Exception as exc:
                logger.warning("Nacos NamingService 关闭异常: %s", exc)
            self._naming_service = None


def main():
    parser = argparse.ArgumentParser(description="Tag 感知的 OmrService 示例客户端")
    parser.add_argument("--method", required=True, choices=[
        "ParseGoldenTemplate",
        "RecognizeByTemplate",
        "VerifyRecognitionRate",
        "ReverifyPaper",
    ])
    parser.add_argument("--tag", default="", help="目标服务实例 tag，空值表示基线实例")
    parser.add_argument("--template-id", type=int, default=0)
    parser.add_argument("--image-url", default="")
    parser.add_argument("--expected", default="", help='VerifyRecognitionRate 用，JSON 对象，如 {"1":"A"}')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    cfg = OmrConfig.from_env()
    client = TagAwareOmrClient(cfg, target_tag=args.tag)

    try:
        if args.method == "ParseGoldenTemplate":
            req = omr_pb2.GoldenTemplateRequest(
                template_id=args.template_id,
                template_image_url=args.image_url,
            )
        elif args.method in ("RecognizeByTemplate", "ReverifyPaper"):
            req = omr_pb2.RecognizeRequest(
                template_id=args.template_id,
                scan_image_url=args.image_url,
            )
        elif args.method == "VerifyRecognitionRate":
            try:
                expected = json.loads(args.expected) if args.expected else {}
            except json.JSONDecodeError as exc:
                parser.error(f"--expected 参数不是合法 JSON: {exc}")
            req = omr_pb2.VerifyRateRequest(
                template_id=args.template_id,
                expected_answers=expected,
                image_urls=[args.image_url] if args.image_url else [],
            )
        else:
            raise ValueError(f"未实现的方法: {args.method}")

        resp = client.call(args.method, req)
        print(resp)
    finally:
        client.close()


if __name__ == "__main__":
    main()
