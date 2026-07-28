from unittest.mock import MagicMock
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from omr_service.api.deps import register_dependencies
from omr_service.api.routers.health import router


@pytest.fixture
def app():
    app = FastAPI()
    app.include_router(router)
    settings = MagicMock()
    settings.sync_timeout_seconds = 60.0
    service = MagicMock()
    register_dependencies(app, settings=settings, service=service, task_registry=MagicMock())
    return app


def test_health_200(app):
    c = TestClient(app)
    r = c.get("/v1/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_health_ready_200(app):
    c = TestClient(app)
    r = c.get("/v1/health/ready")
    assert r.status_code == 200


def test_health_ready_503_when_service_down():
    bad_app = FastAPI()
    bad_app.include_router(router)
    broken_service = MagicMock()
    broken_service.template_store = None
    register_dependencies(bad_app, settings=MagicMock(), service=broken_service, task_registry=MagicMock())
    r = TestClient(bad_app).get("/v1/health/ready")
    assert r.status_code == 503