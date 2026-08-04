"""Tests for omr_service.main entry point."""
from unittest.mock import MagicMock, patch, ANY
import pytest


def test_main_invokes_uvicorn(monkeypatch):
    """main() should call uvicorn.run with the FastAPI app."""
    monkeypatch.setattr("uvicorn.run", MagicMock())
    monkeypatch.setattr("omr_service.main._setup_dependencies", lambda settings: (MagicMock(), MagicMock(), MagicMock()))
    monkeypatch.setattr("omr_service.main._start_nacos", lambda settings: None)
    monkeypatch.setattr("omr_service.main._start_consumer", lambda settings, service: None)
    monkeypatch.setattr("omr_service.main._deregister_nacos", lambda: None)

    from omr_service import main
    main.main()

    import uvicorn
    assert uvicorn.run.called


def test_main_with_omr_settings(monkeypatch):
    """main() should use OmrSettings (new Pydantic)."""
    monkeypatch.setattr("uvicorn.run", MagicMock())
    monkeypatch.setattr("omr_service.main._setup_dependencies", lambda settings: (MagicMock(), MagicMock(), MagicMock()))
    monkeypatch.setattr("omr_service.main._start_nacos", lambda settings: None)
    monkeypatch.setattr("omr_service.main._start_consumer", lambda settings, service: None)
    monkeypatch.setattr("omr_service.main._deregister_nacos", lambda: None)

    from omr_service import main
    # Should not raise
    main.main()
