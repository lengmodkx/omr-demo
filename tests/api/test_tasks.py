from unittest.mock import MagicMock, patch
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from omr_service.api.deps import register_dependencies
from omr_service.api.errors import register_error_handlers
from omr_service.api.routers.tasks import router
from omr_service.api.schemas.enums import TaskStatus, TaskType
from omr_service.core.exceptions import TaskNotFoundError


@pytest.fixture
def app():
    app = FastAPI()
    app.include_router(router)
    register_error_handlers(app)
    settings = MagicMock()
    service = MagicMock()
    task_registry = MagicMock()
    register_dependencies(app, settings=settings, service=service, task_registry=task_registry)
    return app, service, task_registry


def test_create_task_202(app):
    _, _, task_registry = app
    task_registry.write_queued.return_value = None
    c = TestClient(app[0])
    with patch("omr_service.api.routers.tasks.enqueue_job") as mock_enqueue:
        r = c.post("/v1/tasks", json={
            "task_type": "recognize",
            "payload": {"template_id": "t-1", "scan_image_urls": ["http://x.jpg"]},
        })
    assert r.status_code == 202
    assert r.json()["status"] == "queued"
    assert "task_id" in r.json()
    task_registry.write_queued.assert_called_once()
    mock_enqueue.assert_called_once()


def test_get_task_200(app):
    _, _, task_registry = app
    task_registry.get.return_value = {
        "task_id": "t-1",
        "task_type": TaskType.RECOGNIZE,
        "status": TaskStatus.SUCCEEDED,
        "created_at": "2026-07-28T10:00:00Z",
        "finished_at": "2026-07-28T10:00:08Z",
        "result": {"answers": []},
        "error": None,
    }
    c = TestClient(app[0])
    r = c.get("/v1/tasks/t-1")
    assert r.status_code == 200
    assert r.json()["status"] == "succeeded"


def test_get_task_404(app):
    _, _, task_registry = app
    task_registry.get.side_effect = TaskNotFoundError(task_id="missing")
    c = TestClient(app[0])
    r = c.get("/v1/tasks/missing")
    assert r.status_code == 404
    assert r.json()["code"] == 7


def test_create_task_invalid_payload(app):
    c = TestClient(app[0])
    r = c.post("/v1/tasks", json={
        "task_type": "recognize",
        "payload": {},  # missing template_id and scan_image_urls
    })
    assert r.status_code == 400
    assert r.json()["code"] == 6