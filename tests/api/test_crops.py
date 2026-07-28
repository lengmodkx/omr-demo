from pathlib import Path
from unittest.mock import MagicMock
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from omr_service.api.deps import register_dependencies
from omr_service.api.routers.crops import router


@pytest.fixture
def tmp_crop_dir(tmp_path):
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "test.jpg").write_bytes(b"fake-jpg")
    return tmp_path


@pytest.fixture
def app(tmp_crop_dir):
    app = FastAPI()
    app.include_router(router)
    settings = MagicMock()
    settings.crop_output_dir = str(tmp_crop_dir)
    register_dependencies(app, settings=settings, service=MagicMock(), task_registry=MagicMock())
    return app


def test_crop_get_200(app):
    c = TestClient(app)
    r = c.get("/v1/omr_crops/sub/test.jpg")
    assert r.status_code == 200
    assert r.content == b"fake-jpg"


def test_crop_path_traversal_404(app):
    c = TestClient(app)
    r = c.get("/v1/omr_crops/../etc/passwd")
    assert r.status_code in (404, 400)
