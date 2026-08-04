import sys, asyncio
sys.path.insert(0, '.')
from omr_service.nacos_v2_compat import import_client_config, import_naming_service, shared_loop

async def main():
    ClientConfig = import_client_config()
    NacosNamingService = import_naming_service()
    import os
    cc = ClientConfig(
        server_addresses=os.getenv('NACOS_SERVER_ADDR', ''),
        namespace_id=os.getenv('NACOS_NAMESPACE', 'public'),
        username=os.getenv('NACOS_USERNAME', ''),
        password=os.getenv('NACOS_PASSWORD', ''),
        log_level=30,
    )
    svc = await NacosNamingService.create_naming_service(cc)
    try:
        for sn in ['providers:omr.OmrService::', 'omr-service']:
            for grp in ['DUBBO_GROUP', 'DEFAULT_GROUP']:
                info = await svc.grpc_client_proxy.query_instance_of_service(service_name=sn, group_name=grp, clusters='', health_only=False)
                hosts = info.hosts or []
                print(f'{sn}@{grp}: {len(hosts)} hosts')
    finally:
        await svc.shutdown()

loop = shared_loop()
loop.run(main())