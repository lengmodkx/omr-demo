"""gRPC Server 装配"""
import logging
from concurrent import futures

import grpc

from omr_service.config import OmrConfig
from omr_service.loader.image_loader import ImageLoader
from omr_service.loader.template_store import TemplateStore
from omr_service.rpc import omr_pb2_grpc
from omr_service.rpc.omr_service import OmrServiceServicer
from omr_service.worker.pool import WorkerPool

logger = logging.getLogger(__name__)

_TAG_HEADER = "x-service-tag"


class _TagLoggingInterceptor(grpc.ServerInterceptor):
    """读取 gRPC metadata 中的 x-service-tag，用于验证路由是否命中本实例。"""

    def intercept_service(self, continuation, handler_call_details):
        original_handler = continuation(handler_call_details)
        if original_handler is None:
            return None

        # 只包装 unary-unary 方法；流式方法保持原样，避免 unary_unary 为 None 时崩溃
        if original_handler.unary_unary is None:
            return original_handler

        def _wrap_handler(handler):
            def _wrapper(request, servicer_context):
                metadata = dict(servicer_context.invocation_metadata() or [])
                tag = metadata.get(_TAG_HEADER)
                if tag:
                    logger.info("[rpc] 请求携带 x-service-tag=%s", tag)
                return handler(request, servicer_context)

            return _wrapper

        return grpc.unary_unary_rpc_method_handler(
            _wrap_handler(original_handler.unary_unary),
            request_deserializer=original_handler.request_deserializer,
            response_serializer=original_handler.response_serializer,
        )


def create_server(
    cfg: OmrConfig,
    template_store: TemplateStore,
    image_loader: ImageLoader,
    worker_pool: WorkerPool,
) -> grpc.Server:
    """创建并配置 gRPC server"""
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=cfg.worker_count),
        options=[("grpc.max_send_message_length", 50 * 1024 * 1024),
                 ("grpc.max_receive_message_length", 50 * 1024 * 1024)],
        interceptors=[_TagLoggingInterceptor()],
    )
    servicer = OmrServiceServicer(cfg, template_store, image_loader, worker_pool)
    omr_pb2_grpc.add_OmrServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{cfg.dubbo_port}")
    return server
