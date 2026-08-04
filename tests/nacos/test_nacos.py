"""Nacos 注册器与配置中心单元测试"""
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from omr_service.config import OmrSettings
from omr_service.nacos_config import NacosConfigClient, _parse_content
from omr_service.nacos_reg import NacosRegistrator


class TestMetadata(unittest.TestCase):
    def test_build_metadata(self):
        settings = OmrSettings(_env_file=None, nacos_ip="10.0.0.2", http_port=8080)
        reg = NacosRegistrator(settings)
        meta = reg.build_metadata()
        self.assertEqual(meta["protocol"], "http")
        self.assertEqual(meta["port"], "8080")
        self.assertEqual(meta["version"], "2.0.0")
        self.assertEqual(meta["health_check_url"], "http://10.0.0.2:8080/v1/health")
        self.assertNotIn("interface", meta)
        self.assertNotIn("path", meta)

    @patch.dict("os.environ", {"OMR_TAG": "zhangsan"})
    def test_build_metadata_tag_with_value(self):
        reg = NacosRegistrator(OmrSettings(_env_file=None))
        self.assertEqual(reg.build_metadata()["tag"], "zhangsan")


class _SimpleParam:
    """Minimal stand-in for SDK param dataclasses."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class TestNacosRegistrator(unittest.TestCase):
    @patch("omr_service.nacos_reg.import_naming_service")
    @patch("omr_service.nacos_reg.import_client_config")
    @patch("omr_service.nacos_reg.import_register_instance_param")
    @patch("omr_service.nacos_reg.import_deregister_instance_param")
    @patch("omr_service.nacos_reg.shared_loop")
    def test_register_and_deregister(
        self,
        mock_shared_loop,
        mock_dereg_param,
        mock_reg_param,
        mock_cfg_cls,
        mock_naming_cls,
    ):
        settings = OmrSettings(_env_file=None, nacos_ip="10.0.0.2", http_port=8080)
        mock_reg_param.return_value = _SimpleParam
        mock_dereg_param.return_value = _SimpleParam

        mock_service = MagicMock()
        mock_service.register_instance = AsyncMock(return_value=True)
        mock_service.deregister_instance = AsyncMock(return_value=True)
        mock_service.shutdown = AsyncMock()
        mock_naming_cls.return_value.create_naming_service = AsyncMock(return_value=mock_service)

        mock_loop = MagicMock()
        mock_loop.run = MagicMock(side_effect=lambda coro: asyncio.run(coro))
        mock_shared_loop.return_value = mock_loop

        reg = NacosRegistrator(settings)
        self.assertTrue(reg.register())
        calls = mock_service.register_instance.await_args_list
        self.assertEqual(len(calls), 1)
        param = calls[0].args[0]
        self.assertEqual(param.service_name, settings.nacos_service_name)
        self.assertEqual(param.port, settings.http_port)
        self.assertEqual(param.metadata["protocol"], "http")

        self.assertTrue(reg.deregister())
        reg.close()
        mock_service.shutdown.assert_awaited()


def test_register_only_app_level_not_interface_level():
    """接口级注册 providers:omr.OmrService:: 已删除."""
    settings = OmrSettings(_env_file=None)
    settings.nacos_enabled = True
    settings.nacos_service_name = "omr-service"
    settings.http_port = 8080
    reg = NacosRegistrator(settings)
    metadata = reg.build_metadata()
    assert metadata.get("protocol") == "http" or "protocol" in metadata
    assert "interface" not in metadata
    assert "path" not in metadata


class TestConfigParse(unittest.TestCase):
    def test_yaml(self):
        content = "redis:\n  host: 127.0.0.1\n  port: 6379\n"
        parsed = _parse_content(content, "omr-service.yaml")
        self.assertEqual(parsed["redis"]["host"], "127.0.0.1")

    def test_json(self):
        content = '{"key": "value"}'
        parsed = _parse_content(content, "config.json")
        self.assertEqual(parsed["key"], "value")


class TestNacosConfigClient(unittest.TestCase):
    @patch("omr_service.nacos_config.import_config_service")
    @patch("omr_service.nacos_config.import_client_config")
    @patch("omr_service.nacos_config.import_config_param")
    @patch("omr_service.nacos_config.shared_loop")
    def test_load(
        self,
        mock_shared_loop,
        mock_config_param,
        mock_cfg_cls,
        mock_config_service_cls,
    ):
        settings = OmrSettings(_env_file=None)
        mock_config_param.return_value = _SimpleParam

        mock_service = MagicMock()
        mock_service.get_config = AsyncMock(return_value="key: value")
        mock_service.shutdown = AsyncMock()
        mock_config_service_cls.return_value.create_config_service = AsyncMock(return_value=mock_service)

        mock_loop = MagicMock()
        mock_loop.run = MagicMock(side_effect=lambda coro: asyncio.run(coro))
        mock_shared_loop.return_value = mock_loop

        client = NacosConfigClient(settings)
        result = client.load()
        self.assertEqual(result["key"], "value")


if __name__ == "__main__":
    unittest.main()
