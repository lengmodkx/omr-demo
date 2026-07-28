from unittest.mock import MagicMock
import pytest
from omr_service.core.task_registry import TaskRegistry
from omr_service.core.exceptions import TaskNotFoundError
from omr_service.api.schemas.enums import TaskStatus


@pytest.fixture
def mock_redis():
    return MagicMock()


def test_get_task_succeeded(mock_redis):
    mock_redis.hgetall.return_value = {
        "status": "succeeded",
        "task_type": "recognize",
        "created_at": "2026-07-28T10:00:00Z",
        "finished_at": "2026-07-28T10:00:08Z",
        "result": '{"answers": []}',
    }
    reg = TaskRegistry(redis_client=mock_redis, hash_prefix="h:")
    task = reg.get("t-1")
    assert task["status"] == TaskStatus.SUCCEEDED
    assert task["result"]["answers"] == []


def test_get_task_processing(mock_redis):
    mock_redis.hgetall.return_value = {
        "status": "processing",
        "task_type": "recognize",
        "created_at": "2026-07-28T10:00:00Z",
    }
    reg = TaskRegistry(redis_client=mock_redis, hash_prefix="h:")
    task = reg.get("t-1")
    assert task["status"] == TaskStatus.PROCESSING


def test_get_task_not_found(mock_redis):
    mock_redis.hgetall.return_value = {}
    reg = TaskRegistry(redis_client=mock_redis, hash_prefix="h:")
    with pytest.raises(TaskNotFoundError):
        reg.get("missing")


def test_get_task_uses_hash_prefix(mock_redis):
    mock_redis.hgetall.return_value = {}
    reg = TaskRegistry(redis_client=mock_redis, hash_prefix="omr:batch:result:hash")
    with pytest.raises(TaskNotFoundError):
        reg.get("t-1")
    mock_redis.hgetall.assert_called_with("omr:batch:result:hash:t-1")