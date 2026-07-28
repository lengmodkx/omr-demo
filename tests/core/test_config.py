from omr_service.config import OmrSettings


def test_default_http_port():
    s = OmrSettings(_env_file=None)
    assert s.http_port == 8080


def test_env_override(monkeypatch):
    monkeypatch.setenv("OMR_HTTP_PORT", "9999")
    s = OmrSettings(_env_file=None)
    assert s.http_port == 9999


def test_legacy_dubbo_port_alias(monkeypatch):
    monkeypatch.setenv("OMR_LEGACY_DUBBO_PORT", "20884")
    s = OmrSettings(_env_file=None)
    assert s.legacy_dubbo_port == 20884


def test_redis_result_hash_prefix():
    s = OmrSettings(_env_file=None)
    assert s.redis_result_hash_prefix == "omr:batch:result:hash"
