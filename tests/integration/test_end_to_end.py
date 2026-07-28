"""E2E: 启动完整 FastAPI app, 真实 HTTP 调用.

需要: 测试时 Nacos 关闭 (避免副作用), Redis mock.
"""
from unittest.mock import MagicMock
import pytest
from fastapi.testclient import TestClient

from omr_service.api.app import create_app
from omr_service.config import OmrSettings


@pytest.fixture
def e2e_app():
    settings = OmrSettings(_env_file=None)
    settings.nacos_enabled = False
    settings.redis_enabled = False
    settings.consumer_enabled = False

    service = MagicMock()
    service.template_store = MagicMock()
    service.recognize.return_value = {
        "code": 0, "message": "ok", "template_id": "t-1",
        "answers": [], "abnormal": False, "empty_count": 0, "multiple_count": 0, "elapsed_ms": 10,
    }
    service.parse_golden_template.return_value = {
        "code": 0, "message": "ok", "template_id": "t-1",
        "answers": [], "bubble_grid": [], "elapsed_ms": 10,
    }
    service.reverify_paper.return_value = {
        "code": 0, "message": "ok", "template_id": "t-1",
        "answers": [], "abnormal": False, "empty_count": 0, "multiple_count": 0, "elapsed_ms": 10,
    }
    service.verify_recognition_rate.side_effect = Exception("not implemented")

    task_registry = MagicMock()
    app = create_app(settings=settings, service=service, task_registry=task_registry)
    return TestClient(app)


def test_e2e_health_to_recognize(e2e_app):
    r = e2e_app.get("/v1/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

    r = e2e_app.post("/v1/recognize", json={
        "template_id": "t-1",
        "scan_image_urls": ["http://x.jpg"],
    })
    assert r.status_code == 200
    assert r.json()["code"] == 0


def test_e2e_openapi_doc(e2e_app):
    r = e2e_app.get("/v1/openapi.json")
    assert r.status_code == 200
    spec = r.json()
    assert "/v1/recognize" in spec["paths"]
    assert "/v1/templates/parse" in spec["paths"]
    assert "/v1/tasks" in spec["paths"]


def test_e2e_swagger_ui(e2e_app):
    r = e2e_app.get("/v1/docs")
    assert r.status_code == 200
    assert "swagger" in r.text.lower()


def test_e2e_request_id_in_response(e2e_app):
    r = e2e_app.get("/v1/health", headers={"X-Request-ID": "test-123"})
    assert r.status_code == 200
    assert r.headers.get("X-Request-ID") == "test-123"


def test_e2e_recognize_with_personal_info(e2e_app):
    r = e2e_app.post("/v1/recognize", json={
        "template_id": "t-1",
        "scan_image_urls": ["http://x.jpg"],
        "personal_info_region": {"x": 0, "y": 0, "width": 100, "height": 50},
    })
    assert r.status_code == 200
    assert r.json()["code"] == 0


def test_e2e_invalid_payload_returns_400(e2e_app):
    r = e2e_app.post("/v1/recognize", json={"template_id": "t-1"})  # missing scan_image_urls
    assert r.status_code == 400
    assert r.json()["code"] == 6
