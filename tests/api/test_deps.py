from unittest.mock import MagicMock
import pytest
from fastapi import FastAPI, Depends
from fastapi.testclient import TestClient

from omr_service.api.deps import register_dependencies, get_service, get_settings, get_task_registry


@pytest.fixture
def mock_app():
    app = FastAPI()
    settings = MagicMock()
    settings.sync_timeout_seconds = 60.0
    settings.http_port = 8088
    service = MagicMock()
    task_registry = MagicMock()
    register_dependencies(app, settings=settings, service=service, task_registry=task_registry)
    return app, settings, service, task_registry


def test_get_settings(mock_app):
    app, settings, _, _ = mock_app

    @app.get("/s")
    def s(s=Depends(get_settings)):
        return {"http_port": s.http_port}

    c = TestClient(app)
    assert c.get("/s").json()["http_port"] == settings.http_port


def test_get_service(mock_app):
    app, _, service, _ = mock_app

    @app.get("/sv")
    def sv(s=Depends(get_service)):
        return {"ok": True}

    c = TestClient(app)
    assert c.get("/sv").json() == {"ok": True}


def test_get_task_registry(mock_app):
    app, _, _, reg = mock_app

    @app.get("/r")
    def r(reg_=Depends(get_task_registry)):
        return {"ok": True}

    c = TestClient(app)
    assert c.get("/r").json() == {"ok": True}
