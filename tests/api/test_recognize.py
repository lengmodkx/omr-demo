from unittest.mock import MagicMock
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from omr_service.api.deps import register_dependencies
from omr_service.api.errors import register_error_handlers
from omr_service.api.routers.recognize import router
from omr_service.core.exceptions import (
    TemplateNotFoundError,
    ImageLoadError,
)


@pytest.fixture
def app():
    app = FastAPI()
    app.include_router(router)
    register_error_handlers(app)
    settings = MagicMock()
    service = MagicMock()
    register_dependencies(app, settings=settings, service=service, task_registry=MagicMock())
    return app, service


def test_recognize_200(app):
    _, service = app
    service.recognize.return_value = {
        "code": 0, "message": "ok", "template_id": "t-1",
        "answers": [{"question_no": 1, "selected": ["A"], "answer_type": "single", "is_blank": False, "is_multiple": False}],
        "abnormal": False, "empty_count": 0, "multiple_count": 0, "elapsed_ms": 123,
    }

    c = TestClient(app[0])
    r = c.post("/v1/recognize", json={
        "template_id": "t-1",
        "scan_image_urls": ["http://x/y.jpg"],
    })
    assert r.status_code == 200
    assert r.json()["code"] == 0
    assert r.json()["answers"][0]["selected"] == ["A"]


def test_recognize_404_template_not_found(app):
    _, service = app
    service.recognize.side_effect = TemplateNotFoundError(template_id="t-1")

    c = TestClient(app[0])
    r = c.post("/v1/recognize", json={"template_id": "t-1", "scan_image_urls": ["http://x/y.jpg"]})
    assert r.status_code == 404
    assert r.json()["code"] == 4


def test_recognize_502_image_load_error(app):
    _, service = app
    service.recognize.side_effect = ImageLoadError(url="http://x", reason="timeout")

    c = TestClient(app[0])
    r = c.post("/v1/recognize", json={"template_id": "t-1", "scan_image_urls": ["http://x/y.jpg"]})
    assert r.status_code == 502
    assert r.json()["code"] == 5


def test_recognize_400_missing_fields(app):
    c = TestClient(app[0])
    r = c.post("/v1/recognize", json={"template_id": "t-1"})
    assert r.status_code == 400
    assert r.json()["code"] == 6


def test_recognize_400_invalid_url_list(app):
    c = TestClient(app[0])
    r = c.post("/v1/recognize", json={"template_id": "t-1", "scan_image_urls": []})
    assert r.status_code == 400
