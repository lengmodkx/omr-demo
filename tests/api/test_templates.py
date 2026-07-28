from unittest.mock import MagicMock
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from omr_service.api.deps import register_dependencies
from omr_service.api.errors import register_error_handlers
from omr_service.api.routers.templates import router
from omr_service.core.exceptions import InvalidRequestError, InternalError


@pytest.fixture
def app():
    app = FastAPI()
    app.include_router(router)
    register_error_handlers(app)
    settings = MagicMock()
    service = MagicMock()
    register_dependencies(app, settings=settings, service=service, task_registry=MagicMock())
    return app, service


def test_parse_template_200(app):
    _, service = app
    service.parse_golden_template.return_value = {
        "code": 0, "message": "ok", "template_id": "t-1",
        "answers": [], "bubble_grid": [], "elapsed_ms": 100,
    }
    c = TestClient(app[0])
    r = c.post("/v1/templates/parse", json={
        "template_id": "t-1",
        "template_image_url": "http://x/tpl.jpg",
        "columns": [{"column_id": "c1", "column_index": 0, "question_start": 1, "question_count": 5}],
    })
    assert r.status_code == 200
    assert r.json()["code"] == 0


def test_parse_template_400_empty_columns(app):
    c = TestClient(app[0])
    r = c.post("/v1/templates/parse", json={
        "template_id": "t-1",
        "template_image_url": "http://x/tpl.jpg",
        "columns": [],
    })
    assert r.status_code == 400
    assert r.json()["code"] == 6


def test_verify_recognition_rate_500(app):
    _, service = app
    service.verify_recognition_rate.side_effect = InternalError("verify_recognition_rate 暂未通过 HTTP 暴露")
    c = TestClient(app[0])
    r = c.post("/v1/verify_recognition_rate", json={})
    assert r.status_code == 500
    assert r.json()["code"] == 99


def test_reverify_paper_delegates(app):
    _, service = app
    service.reverify_paper.return_value = {"code": 0, "message": "ok", "template_id": "t-1", "answers": [], "abnormal": False, "empty_count": 0, "multiple_count": 0, "elapsed_ms": 50}
    c = TestClient(app[0])
    r = c.post("/v1/reverify_paper", json={
        "template_id": "t-1",
        "scan_image_urls": ["http://x/y.jpg"],
    })
    assert r.status_code == 200
    assert r.json()["code"] == 0
