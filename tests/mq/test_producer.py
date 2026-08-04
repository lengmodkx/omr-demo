"""Tests for omr_service.mq.producer dual-write (Stream + Hash).

Plan A Task 16: verify enqueue_job 写两个地方: Redis Stream + Redis Hash.
"""
from unittest.mock import MagicMock, patch

import pytest

from omr_service.config import OmrConfig
from omr_service.mq import producer as producer_mod
from omr_service.mq.producer import enqueue_job


@pytest.fixture
def cfg():
    return OmrConfig.from_env()


@pytest.fixture
def patch_producer_and_client():
    """Patch MqProducer + MqClient; return (mock_producer, mock_client).

    因为我们用 MagicMock 替换整个 MqProducer, 所以 producer 构造里
    的 MqClient(cfg) 不会被调用. 整条 enqueue_job 路径里, MqClient()
    只会被实例化一次 (hash writer).
    """
    mock_producer = MagicMock(name="MqProducer_instance")
    # real MqProducer.connect() returns self; mirror that
    mock_producer.connect.return_value = mock_producer

    mock_client = MagicMock(name="hash_MqClient_instance")
    mock_redis = MagicMock(name="hash_redis")
    mock_client.redis = mock_redis
    mock_client.connect.return_value = mock_client

    producer_p = patch.object(producer_mod, "MqProducer", return_value=mock_producer)
    client_p = patch.object(producer_mod, "MqClient", return_value=mock_client)
    producer_p.start()
    client_p.start()

    yield mock_producer, mock_client, mock_redis

    producer_p.stop()
    client_p.stop()


def test_enqueue_job_writes_to_stream_and_hash(cfg, patch_producer_and_client):
    """verify enqueue_job 写两个地方: Stream + Hash."""
    mock_producer, mock_client, mock_redis = patch_producer_and_client

    task_id = enqueue_job(
        task_type="recognize",
        payload={"template_id": "t-1", "scan_image_urls": ["http://x.jpg"]},
        task_id="t-1",
        cfg=cfg,
    )

    assert task_id == "t-1"

    # Verify Stream write via MqProducer.send_job
    mock_producer.send_job.assert_called_once()
    sent_payload = mock_producer.send_job.call_args.kwargs["payload"]
    assert sent_payload["task_id"] == "t-1"
    assert sent_payload["job_type"] == "recognize"
    assert sent_payload["template_id"] == "t-1"
    assert sent_payload["scan_image_urls"] == ["http://x.jpg"]

    # Verify Hash write via Redis hset
    mock_redis.hset.assert_called_once()
    hash_key = mock_redis.hset.call_args.args[0]
    assert hash_key == "omr:batch:result:hash:t-1"

    mapping = mock_redis.hset.call_args.kwargs["mapping"]
    assert mapping["status"] == "queued"
    assert mapping["task_type"] == "recognize"
    assert "created_at" in mapping
    assert "payload" in mapping


def test_enqueue_job_generates_task_id_if_not_provided(cfg, patch_producer_and_client):
    """如果 task_id 未传, 生成 uuid4."""
    mock_producer, mock_client, mock_redis = patch_producer_and_client

    task_id = enqueue_job(
        task_type="recognize",
        payload={"template_id": "t-1"},
        cfg=cfg,
    )

    # uuid4 格式: 36 字符, 4 dashes
    assert task_id is not None
    assert isinstance(task_id, str)
    assert len(task_id) == 36
    assert task_id.count("-") == 4

    # Stream sent_payload 应使用同一个 task_id
    sent_payload = mock_producer.send_job.call_args.kwargs["payload"]
    assert sent_payload["task_id"] == task_id

    # Hash key 使用同一个 task_id
    hash_key = mock_redis.hset.call_args.args[0]
    assert hash_key == f"omr:batch:result:hash:{task_id}"


def test_enqueue_job_signature_accepts_positional_and_keyword(cfg, patch_producer_and_client):
    """签名兼容: (task_type, payload, task_id=None, *, hash_prefix=..., cfg=...)."""
    mock_producer, mock_client, mock_redis = patch_producer_and_client

    # positional
    tid = enqueue_job("recognize", {"a": 1}, "explicit-id", cfg=cfg)
    assert tid == "explicit-id"

    # keyword + custom hash_prefix
    mock_redis.reset_mock()
    tid2 = enqueue_job(
        task_type="crop",
        payload={"x": 2},
        task_id="kw-id",
        hash_prefix="custom:hash",
        cfg=cfg,
    )
    assert tid2 == "kw-id"

    # verify custom hash_prefix was applied
    hash_key = mock_redis.hset.call_args.args[0]
    assert hash_key == "custom:hash:kw-id"


def test_enqueue_job_closes_connections(cfg, patch_producer_and_client):
    """verify 两个连接 (producer + hash client) 都被 close() 释放."""
    mock_producer, mock_client, mock_redis = patch_producer_and_client

    enqueue_job("recognize", {"a": 1}, "tid", cfg=cfg)

    # Producer close
    mock_producer.close.assert_called_once()
    # Hash writer client close
    mock_client.close.assert_called_once()


def test_enqueue_job_continues_when_hash_write_fails(cfg, patch_producer_and_client):
    """Hash 写失败只记录 warning, 不影响 Stream 已写入 (异步消费者仍能处理)."""
    mock_producer, mock_client, mock_redis = patch_producer_and_client
    mock_redis.hset.side_effect = RuntimeError("redis down")

    task_id = enqueue_job("recognize", {"a": 1}, "tid-1", cfg=cfg)
    assert task_id == "tid-1"

    # Stream write 已发生
    mock_producer.send_job.assert_called_once()
    # Hash 写过 (但抛异常), Hash client 仍被 close
    mock_redis.hset.assert_called_once()
    mock_client.close.assert_called_once()
