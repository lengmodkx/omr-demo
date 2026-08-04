import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from omr_service.api.errors import register_error_handlers
from omr_service.core.exceptions import (
    OmrError,
    TemplateNotFoundError,
    ImageLoadError,
    InvalidRequestError,
    TaskNotFoundError,
    InternalError,
)


@pytest.fixture
def app():
    app = FastAPI()
    register_error_handlers(app)

    @app.get("/raise-omr")
    def raise_omr():
        raise TemplateNotFoundError(template_id="t-1")

    @app.get("/raise-image")
    def raise_image():
        raise ImageLoadError(url="http://x", reason="timeout")

    @app.get("/raise-invalid")
    def raise_invalid():
        raise InvalidRequestError(field="foo")

    @app.get("/raise-task")
    def raise_task():
        raise TaskNotFoundError(task_id="t-1")

    @app.get("/raise-internal")
    def raise_internal():
        raise InternalError(reason="paddle failed")

    @app.get("/raise-unknown")
    def raise_unknown():
        raise RuntimeError("boom")

    return app


@pytest.fixture
def client(app):
    return TestClient(app, raise_server_exceptions=False)


def test_omr_error_404(client):
    r = client.get("/raise-omr")
    assert r.status_code == 404
    assert r.json()["code"] == 4
    assert "t-1" in r.json()["message"]


def test_image_load_error_502(client):
    r = client.get("/raise-image")
    assert r.status_code == 502
    assert r.json()["code"] == 5


def test_invalid_request_400(client):
    r = client.get("/raise-invalid")
    assert r.status_code == 400
    assert r.json()["code"] == 6


def test_task_not_found_404(client):
    r = client.get("/raise-task")
    assert r.status_code == 404
    assert r.json()["code"] == 7


def test_internal_error_500(client):
    r = client.get("/raise-internal")
    assert r.status_code == 500
    assert r.json()["code"] == 99


def test_unknown_exception_500(client):
    r = client.get("/raise-unknown")
    assert r.status_code == 500
    assert r.json()["code"] == 99