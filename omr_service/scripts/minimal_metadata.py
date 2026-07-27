"""极简 metadata 测试：只保留 Dubbo 3.3.6 InterfaceRouter 必需的几个字段。

如果这样能通，就是多余字段干扰；
如果这样还不通，就是 Dubbo 3.3.6 接口级对 Python 服务有更深的不兼容。
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from omr_service.nacos_v2_compat import (
    import_client_config, import_naming_service, shared_loop,
    import_register_instance_param, import_deregister_instance_param,
)


# 极简 metadata：只保留 Dubbo 3 InterfaceRouter 必需的
MINIMAL_METADATA = {
    # 必须：
    "side": "provider",
    "application": "omr-service",
    "protocol": "tri",
    # 接口级必需：
    "interface": "omr.OmrService",
    "path": "omr.OmrService",
    "version": "",
    "group": "",
    "methods": "parseGoldenTemplate,recognizeByTemplate,verifyRecognitionRate,reverifyPaper",
    # 试试看 Java 端有没有的、可能需要的：
    "release": "3.3.6",
    # 故意不写：dubbo / deprecated / dynamic / generic / logger / category / service-name-mapping /
    # metadata-type / side / prefer.serialization / tri.service / tag
    # 看哪些是 Dubbo 3.3.6 InterfaceRouter 真正必要的
}


async def main():
    ClientConfig = import_client_config()
    NacosNamingService = import_naming_service()
    RegParam = import_register_instance_param()
    DeRegParam = import_deregister_instance_param()
    cc = ClientConfig(
        server_addresses="39.153.154.183:8848",
        namespace_id="8c4541fd-870e-414d-bdee-72cab49fe8d2",
        username="nacos",
        password="lemon2judy",
        log_level=30,
    )
    svc = await NacosNamingService.create_naming_service(cc)
    try:
        # 先清掉
        for sn in ("providers:omr.OmrService::", "omr-service"):
            await svc.deregister_instance(DeRegParam(
                service_name=sn, group_name="DUBBO_GROUP",
                ip="10.135.4.92", port=20884, ephemeral=True,
            ))
        await asyncio.sleep(2)
        # 再用 MINIMAL 重新注册
        for sn in ("providers:omr.OmrService::", "omr-service"):
            ok = await svc.register_instance(RegParam(
                service_name=sn, group_name="DUBBO_GROUP",
                ip="10.135.4.92", port=20884,
                metadata=MINIMAL_METADATA,
                healthy=True, enabled=True, weight=1.0, ephemeral=True,
            ))
            print(f"register {sn} -> {ok}")
    finally:
        await svc.shutdown()


if __name__ == "__main__":
    loop = shared_loop()
    loop.run(main())
