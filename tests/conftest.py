# tests/conftest.py
import pytest

@pytest.fixture
def anyio_backend():
    return "asyncio"
