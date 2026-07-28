"""OMR 服务主入口.

启动流程:
1. 加载配置 (Nacos > env > default)
2. 初始化组件 (TemplateStore, ImageLoader, WorkerPool, OmrService, TaskRegistry)
3. 启动后台线程 (Redis Stream consumer, Nacos 注册/监听)
4. 启动 uvicorn (FastAPI HTTP 入口)
5. 优雅退出 (deregister Nacos, 停 consumer)
"""
from __future__ import annotations

import logging
import signal
import sys
import threading
from typing import Any

import uvicorn

from omr_service.api.app import create_app
from omr_service.config import OmrSettings, load_settings
from omr_service.core.service import OmrService
from omr_service.core.task_registry import TaskRegistry

logger = logging.getLogger(__name__)


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def _setup_dependencies(settings: OmrSettings) -> tuple[OmrService, TaskRegistry, dict]:
    """初始化核心组件."""
    from omr_service.engine.ocr import PersonalInfoOcr
    from omr_service.engine.cropper import SubjectiveCropper
    from omr_service.loader.image_loader import ImageLoader
    from omr_service.loader.template_store import TemplateStore
    from omr_service.worker.pool import WorkerPool

    template_store = TemplateStore(ttl_seconds=settings.template_ttl_seconds)
    image_loader = ImageLoader(max_bytes=settings.image_max_bytes)
    worker_pool = WorkerPool(size=settings.worker_pool_size)
    ocr_engine = PersonalInfoOcr()  # 懒加载
    cropper = SubjectiveCropper(output_dir=settings.crop_output_dir, base_url=settings.crop_base_url)

    service = OmrService(
        template_store=template_store,
        image_loader=image_loader,
        worker_pool=worker_pool,
        ocr_engine=ocr_engine,
        cropper=cropper,
        sync_timeout_seconds=settings.sync_timeout_seconds,
    )

    # 复用 Redis client
    if settings.redis_enabled:
        try:
            from omr_service.mq.client import get_redis_client
            redis_client = get_redis_client()
        except Exception as e:
            logger.warning("Redis client init failed: %s, task_registry will be None", e)
            redis_client = None
    else:
        redis_client = None

    task_registry = TaskRegistry(
        redis_client=redis_client,
        hash_prefix=settings.redis_result_hash_prefix,
    )

    return service, task_registry, {"template_store": template_store}


def _start_consumer(settings: OmrSettings, service: OmrService) -> threading.Thread | None:
    """启动 Redis Stream consumer 线程."""
    if not settings.consumer_enabled:
        logger.info("OMR_CONSUMER_ENABLED=false, skip consumer")
        return None
    if not settings.redis_enabled:
        logger.warning("redis_enabled=false, cannot start consumer")
        return None

    try:
        from omr_service.mq.consumer import start_consumer_thread
        thread = start_consumer_thread(service=service, settings=settings)
        logger.info("Redis Stream consumer started")
        return thread
    except Exception as e:
        logger.warning("Failed to start consumer: %s", e)
        return None


def _start_nacos(settings: OmrSettings) -> None:
    """启动 Nacos 注册 + 配置监听."""
    if not settings.nacos_enabled:
        logger.info("OMR_NACOS_ENABLED=false, skip nacos")
        return
    try:
        from omr_service.nacos_reg import NacosRegistrator
        registrator = NacosRegistrator(settings)
        if registrator.register():
            logger.info("Nacos registered: %s", settings.nacos_service_name)
    except Exception as e:
        logger.warning("Nacos registration failed: %s", e)


def _deregister_nacos() -> None:
    try:
        from omr_service.nacos_reg import deregister_all
        deregister_all()
    except Exception as e:
        logger.warning("Nacos deregister failed: %s", e)


def main() -> None:
    settings = load_settings()
    _configure_logging(settings.log_level)
    logger.info("Starting OMR service on %s:%s", settings.http_host, settings.http_port)

    service, task_registry, _ = _setup_dependencies(settings)
    app = create_app(settings=settings, service=service, task_registry=task_registry)

    consumer_thread = _start_consumer(settings, service)
    _start_nacos(settings)

    def shutdown_handler(signum, frame):
        logger.info("Received signal %s, shutting down", signum)
        _deregister_nacos()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    uvicorn.run(
        app,
        host=settings.http_host,
        port=settings.http_port,
        workers=1,  # PaddleOCR 显存约束
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
