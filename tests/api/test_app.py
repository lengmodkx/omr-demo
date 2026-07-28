from unittest.mock import MagicMock
from omr_service.api.app import create_app
from omr_service.config import OmrSettings


def _all_paths(app):
    """Collect all route paths from the app, including nested router routes."""
    paths = []
    for r in app.routes:
        if hasattr(r, "path"):
            paths.append(r.path)
        elif hasattr(r, "original_router"):
            for sub in r.original_router.routes:
                if hasattr(sub, "path"):
                    paths.append(sub.path)
    return paths


def test_create_app_routes_health():
    settings = OmrSettings(_env_file=None)
    settings.nacos_enabled = False
    settings.redis_enabled = False
    service = MagicMock()
    task_registry = MagicMock()
    app = create_app(settings=settings, service=service, task_registry=task_registry)
    routes = _all_paths(app)
    assert "/v1/health" in routes
    assert "/v1/health/ready" in routes


def test_create_app_includes_api_routes():
    settings = OmrSettings(_env_file=None)
    settings.nacos_enabled = False
    settings.redis_enabled = False
    service = MagicMock()
    task_registry = MagicMock()
    app = create_app(settings=settings, service=service, task_registry=task_registry)
    routes = _all_paths(app)
    assert "/v1/recognize" in routes
    assert "/v1/templates/parse" in routes
    assert "/v1/tasks" in routes
    assert "/v1/openapi.json" in routes
    assert "/v1/docs" in routes


def test_create_app_health_200():
    from fastapi.testclient import TestClient
    settings = OmrSettings(_env_file=None)
    settings.nacos_enabled = False
    settings.redis_enabled = False
    service = MagicMock()
    service.template_store = MagicMock()
    task_registry = MagicMock()
    app = create_app(settings=settings, service=service, task_registry=task_registry)
    c = TestClient(app)
    r = c.get("/v1/health")
    assert r.status_code == 200
