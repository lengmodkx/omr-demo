import sys, asyncio
sys.path.insert(0, '.')
from omr_service.nacos_v2_compat import import_client_config, import_naming_service, shared_loop

async def main():
    ClientConfig = import_client_config()
    NacosNamingService = import_naming_service()
    cc = ClientConfig(server_addresses='39.153.154.183:8848', namespace_id='8c4541fd-870e-414d-bdee-72cab49fe8d2', username='nacos', password='lemon2judy', log_level=30)
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