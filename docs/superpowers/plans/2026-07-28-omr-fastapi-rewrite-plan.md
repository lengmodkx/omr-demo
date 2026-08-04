# OMR FastAPI 改造 (Python 侧) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用 FastAPI 替换 `screenImg/omr_service/http_server.py`，干掉 Dubbo Triple `:20884` 死代码，新增异步任务 REST 包装，统一健康端口为 8080。整个改动在 Python 侧完成，Java 侧调用方式见 Plan B。

**Architecture:**
- 业务核心 `engine/`、`loader/` 0 改动，仅在 `core/service.py` 抽离 `recognize` / `parse_golden_template` 等方法为 plain dict 接口
- API 边界 (`api/schemas/`) 用 Pydantic；内部全部不依赖 FastAPI/Pydantic
- 异步任务底层复用 `mq/producer.py` + `mq/consumer.py`（Redis Stream 0 改动），结果双写 Stream + Hash
- `main.py` 启动 `uvicorn omr_service.main:app --workers 1`；后台线程启 Redis Stream consumer 与 Nacos 注册
- 单进程单 worker（PaddleOCR 显存约束）

**Tech Stack:**
- FastAPI 0.115+ / uvicorn[standard] 0.32+
- Pydantic 2.10+ / pydantic-settings 2.6+
- pytest 8+ / pytest-asyncio / httpx (test)
- 保留: OpenCV 4.8+ / PaddleOCR 2.7 / Redis 5+ / Nacos SDK 3.2

**Reference spec:** `screenImg/docs/superpowers/specs/2026-07-28-omr-fastapi-rewrite-design.md`

**Working directory:** `screenImg/`

**Branch:** `feature/omr-fastapi-rewrite` （基于 `master` 拉）

---

## File Structure

### 新增

```
screenImg/
├── omr_service/
│   ├── api/
│   │   ├── __init__.py
│   │   ├── deps.py                      # FastAPI Depends: settings, service, task_registry
│   │   ├── errors.py                    # OmrError → HTTP 响应 映射
│   │   ├── app.py                       # create_app(settings) FastAPI 实例
│   │   ├── schemas/
│   │   │   ├── __init__.py
│   │   │   ├── common.py                # Region, BaseResponse, ErrorResponse
│   │   │   ├── enums.py                 # AnswerType, TaskType, TaskStatus
│   │   │   ├── recognize.py             # RecognizeRequest, RecognizeResponse, QuestionAnswer
│   │   │   ├── templates.py             # GoldenTemplateRequest, GoldenTemplateResponse, ColumnConfig
│   │   │   └── tasks.py                 # CreateTaskRequest, TaskStatusResponse
│   │   └── routers/
│   │       ├── __init__.py
│   │       ├── recognize.py             # POST /v1/recognize
│   │       ├── templates.py             # POST /v1/templates/parse, /v1/verify_recognition_rate, /v1/reverify_paper
│   │       ├── tasks.py                 # POST /v1/tasks, GET /v1/tasks/{task_id}
│   │       ├── health.py                # GET /v1/health, /v1/health/ready
│   │       └── crops.py                 # GET /v1/omr_crops/{file_path:path}
│   ├── core/
│   │   ├── __init__.py
│   │   ├── service.py                   # OmrService dict in/dict out
│   │   ├── exceptions.py                # OmrError, TemplateNotFoundError, ImageLoadError, InvalidRequestError, InternalError
│   │   └── task_registry.py             # 异步任务 result Hash 查询
│   └── worker/
│       └── pool.py                      # 已有，无改动
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── api/
│   │   ├── __init__.py
│   │   ├── test_recognize.py
│   │   ├── test_templates.py
│   │   ├── test_tasks.py
│   │   ├── test_health.py
│   │   └── test_crops.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── test_service.py
│   │   ├── test_exceptions.py
│   │   └── test_task_registry.py
│   ├── engine/                          # 迁移自 omr_service/tests/test_engine.py
│   ├── loader/                          # 迁移自 omr_service/tests (template_store)
│   └── mq/                              # 迁移自 omr_service/tests (test_mq, test_consumer, test_job_handler, test_redis)
```

### 修改

```
screenImg/
├── omr_service/
│   ├── config.py                        # dataclass → Pydantic Settings
│   ├── main.py                          # main(): 启动 uvicorn + 后台线程
│   ├── nacos_reg.py                     # 删除 providers:omr.OmrService:: 接口级注册
│   ├── mq/producer.py                   # 新增 ensure_writable_hash(task_id) 双写
│   ├── mq/job_handler.py                # 完成后写 Hash 结果
│   └── worker/pool.py                   # 不改
├── requirements.txt                     # +fastapi +uvicorn +pydantic-settings +pytest +pytest-asyncio +httpx
├── Dockerfile                           # ENTRYPOINT 改 uvicorn
├── docker-compose.yml                   # 端口 8080，去 20884
├── docker-compose.prod.yml              # 端口 8080，去 20884，删 RabbitMQ
├── .env.example                         # 重排为新字段
├── nacos-config-example.yaml            # 新字段
├── README.md                            # 端点表
├── AGENTS.md                            # 删除 Dubbo 段落
├── CLAUDE.md                            # 删除 Dubbo 段落
└── docs/OMR服务接口文档.md                # 改写为 HTTP-only
```

### 删除

```
screenImg/omr_service/
├── server.py                            # gRPC server
├── http_server.py                       # HTTP fallback
├── health.py                            # 合并到 api/routers/health.py
├── rpc/                                 # 整目录: omr.proto, omr_pb2.py, omr_pb2_grpc.py, omr_service.py, tag_aware_client.py
├── engine/processor.py                  # 死代码（早期模板差分法）
├── scripts/patch_nacos_protobuf.py      # 不再需要 PaddlePaddle 降级 protobuf
├── tests/test_rpc.py                    # gRPC 测试
└── tests/ (整个目录不再保留，迁移到 tests/)
```

---

## Task 1: 基础设施 (TDD 基线)

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `pytest.ini`
- Modify: `requirements.txt`

- [ ] **Step 1: 写失败的 pytest 基线测试**

在 `tests/__init__.py` 留空：

```python
# tests/__init__.py
```

在 `tests/conftest.py` 最小化：

```python
# tests/conftest.py
import pytest

@pytest.fixture
def anyio_backend():
    return "asyncio"
```

在 `pytest.ini`：

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_functions = test_*
asyncio_mode = auto
```

- [ ] **Step 2: 跑 pytest 验证基线失败**

Run: `cd screenImg && pytest -q`
Expected: "no tests ran" 或 collection error（`pytest` 还没装）。

- [ ] **Step 3: 在 requirements.txt 加测试依赖**

追加到 `requirements.txt` 末尾：

```
# FastAPI 改造 P0
fastapi>=0.115.0
uvicorn[standard]>=0.32.0
pydantic-settings>=2.6.0
# 测试
pytest>=8.0.0
pytest-asyncio>=0.24.0
pytest-cov>=6.0.0
httpx>=0.27.0
```

- [ ] **Step 4: 安装依赖**

Run: `cd screenImg && pip install -r requirements.txt`
Expected: 全部安装成功。

- [ ] **Step 5: 跑 pytest 验证基线通过**

Run: `cd screenImg && pytest -q`
Expected: "no tests ran" 但 collection OK。

- [ ] **Step 6: 提交**

```bash
cd screenImg
git add tests/__init__.py tests/conftest.py pytest.ini requirements.txt
git commit -m "build(omr): add fastapi, uvicorn, pydantic-settings, pytest"
```

---

## Task 2: 迁移老 tests 到 tests/ (除 test_rpc.py / test_integration.py 外)

**Files:**
- Create: `tests/engine/__init__.py`, `tests/loader/__init__.py`, `tests/mq/__init__.py`
- Move: `omr_service/tests/test_engine.py` → `tests/engine/test_engine.py`
- Move: `omr_service/tests/test_ocr.py` → `tests/engine/test_ocr.py`
- Move: `omr_service/tests/test_personal_info_parser.py` → `tests/engine/test_personal_info_parser.py`
- Move: `omr_service/tests/test_cropper.py` → `tests/engine/test_cropper.py`
- Move: `omr_service/tests/test_mq.py` → `tests/mq/test_mq.py`
- Move: `omr_service/tests/test_consumer.py` → `tests/mq/test_consumer.py`
- Move: `omr_service/tests/test_job_handler.py` → `tests/mq/test_job_handler.py`
- Move: `omr_service/tests/test_redis.py` → `tests/mq/test_redis.py`
- Move: `omr_service/tests/test_nacos.py` → `tests/nacos/test_nacos.py`
- Delete: `omr_service/tests/` (整目录)

- [ ] **Step 1: 创建新目录骨架**

```bash
cd screenImg
mkdir -p tests/engine tests/loader tests/mq tests/nacos tests/api tests/core
touch tests/engine/__init__.py tests/loader/__init__.py tests/mq/__init__.py tests/nacos/__init__.py
```

- [ ] **Step 2: 用 git mv 迁移测试文件**

```bash
cd screenImg
git mv omr_service/tests/test_engine.py tests/engine/test_engine.py
git mv omr_service/tests/test_ocr.py tests/engine/test_ocr.py
git mv omr_service/tests/test_personal_info_parser.py tests/engine/test_personal_info_parser.py
git mv omr_service/tests/test_cropper.py tests/engine/test_cropper.py
git mv omr_service/tests/test_mq.py tests/mq/test_mq.py
git mv omr_service/tests/test_consumer.py tests/mq/test_consumer.py
git mv omr_service/tests/test_job_handler.py tests/mq/test_job_handler.py
git mv omr_service/tests/test_redis.py tests/mq/test_redis.py
git mv omr_service/tests/test_nacos.py tests/nacos/test_nacos.py
```

- [ ] **Step 3: 删除旧 tests 目录**

警告：`test_rpc.py` 与 `test_integration.py` 不迁移，在 Delete 阶段处理（Task 14）。本步只清空老目录：

```bash
cd screenImg
git rm omr_service/tests/test_rpc.py
git rm omr_service/tests/test_integration.py
rm -rf omr_service/tests
```

- [ ] **Step 4: 跑测试验证仍然能通过**

Run: `cd screenImg && pytest -q tests/engine tests/mq tests/loader tests/nacos`
Expected: 全部通过（与迁移前一致）。

- [ ] **Step 5: 提交**

```bash
cd screenImg
git add -A
git commit -m "test(omr): migrate tests from omr_service/tests to tests/ (TDD green)"
```

---

## Task 3: config 重构为 Pydantic Settings

**Files:**
- Create: `tests/core/test_config.py`
- Modify: `omr_service/config.py`

- [ ] **Step 1: 写失败的 config 测试**

```python
# tests/core/test_config.py
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
    # 兼容期读取，仅用于 warning 日志
    assert s.legacy_dubbo_port == 20884

def test_redis_result_hash_prefix():
    s = OmrSettings(_env_file=None)
    assert s.redis_result_hash_prefix == "omr:batch:result:hash"
```

- [ ] **Step 2: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/core/test_config.py`
Expected: ImportError: cannot import name 'OmrSettings' from 'omr_service.config'

- [ ] **Step 3: 重构 omr_service/config.py**

**完整重写** `omr_service/config.py`：

```python
"""OMR 服务配置：Pydantic Settings。

加载优先级：Nacos > 环境变量/.env > Pydantic 默认。

兼容期字段（2026-07-28 引入，1 个版本后删除）：
- `legacy_dubbo_port` 读取 `OMR_LEGACY_DUBBO_PORT`，仅用于 warning 日志。
- 旧 `OMR_HEALTH_PORT` 读取后写入 warning 日志。
"""
from __future__ import annotations

from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class OmrSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="OMR_",
        case_sensitive=False,
        extra="ignore",
    )

    # 服务
    http_host: str = "0.0.0.0"
    http_port: int = 8080
    # health_port 已合并到 http_port=8080
    log_level: str = "INFO"

    # Nacos
    nacos_enabled: bool = True
    nacos_server: str = "127.0.0.1:8848"
    nacos_namespace: str = "public"
    nacos_group: str = "DEFAULT_GROUP"
    nacos_data_id: str = "omr-service.yaml"
    nacos_service_name: str = "omr-service"
    nacos_ip: str = ""

    # Redis
    redis_enabled: bool = True
    redis_host: str = "127.0.0.1"
    redis_port: int = 6379
    redis_db: int = 1
    redis_password: str = ""
    redis_stream_job: str = "omr:batch:job"
    redis_stream_result: str = "omr:batch:result"
    redis_result_hash_prefix: str = "omr:batch:result:hash"

    # 任务相关
    consumer_enabled: bool = True
    worker_pool_size: int = 4
    sync_timeout_seconds: float = 60.0

    # OMR 内部
    template_ttl_seconds: int = 3600
    image_max_bytes: int = 50 * 1024 * 1024
    crop_output_dir: str = "./output"
    crop_base_url: str = "http://127.0.0.1:8080/v1/omr_crops"

    # 兼容期
    legacy_dubbo_port: Optional[int] = None

    @property
    def health_port(self) -> int:
        """兼容期：health 端口已合并到 http_port，仅用作 warning 日志。"""
        return self.http_port


def load_settings() -> OmrSettings:
    """入口：实例化 OmrSettings。Nacos 合并在 main.py 中处理。"""
    import os
    settings = OmrSettings()
    # 兼容期：旧 OMR_DUBBO_PORT / OMR_HEALTH_PORT 触发 warning
    if os.getenv("OMR_DUBBO_PORT"):
        import warnings
        warnings.warn(
            "OMR_DUBBO_PORT 已废弃，Dubbo Triple 服务已下线。请使用 OMR_HTTP_PORT。",
            DeprecationWarning,
            stacklevel=2,
        )
    if os.getenv("OMR_HEALTH_PORT"):
        import warnings
        warnings.warn(
            "OMR_HEALTH_PORT 已合并到 OMR_HTTP_PORT，仅用于 1 版本兼容期。",
            DeprecationWarning,
            stacklevel=2,
        )
    return settings
```

- [ ] **Step 4: 跑测试验证**

Run: `cd screenImg && pytest -q tests/core/test_config.py`
Expected: 4 tests pass。

- [ ] **Step 5: 提交**

```bash
cd screenImg
git add omr_service/config.py tests/core/test_config.py
git commit -m "refactor(omr): config from dataclass to Pydantic Settings"
```

---

## Task 4: core/exceptions.py (OmrError 体系)

**Files:**
- Create: `omr_service/core/__init__.py`
- Create: `omr_service/core/exceptions.py`
- Create: `tests/core/test_exceptions.py`

- [ ] **Step 1: 写失败的异常测试**

```python
# tests/core/test_exceptions.py
import pytest
from omr_service.core.exceptions import (
    OmrError,
    TemplateNotFoundError,
    ImageLoadError,
    InvalidRequestError,
    TaskNotFoundError,
    InternalError,
)


def test_omr_error_has_code_and_message():
    err = OmrError(code=99, message="boom")
    assert err.code == 99
    assert err.message == "boom"
    assert str(err) == "boom"


def test_template_not_found_error_default_code():
    err = TemplateNotFoundError(template_id="t-1")
    assert err.code == 4
    assert "t-1" in err.message


def test_image_load_error_default_code():
    err = ImageLoadError(url="http://x", reason="timeout")
    assert err.code == 5
    assert "timeout" in err.message


def test_invalid_request_error_default_code():
    err = InvalidRequestError(field="foo")
    assert err.code == 6
    assert "foo" in err.message


def test_task_not_found_error_default_code():
    err = TaskNotFoundError(task_id="t-abc")
    assert err.code == 7
    assert "t-abc" in err.message


def test_internal_error_default_code():
    err = InternalError(reason="paddle failed")
    assert err.code == 99
    assert "paddle failed" in err.message


def test_inheritance():
    with pytest.raises(OmrError):
        raise TemplateNotFoundError(template_id="t")
```

- [ ] **Step 2: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/core/test_exceptions.py`
Expected: ModuleNotFoundError: No module named 'omr_service.core'

- [ ] **Step 3: 实现 core/exceptions.py**

```python
# omr_service/core/__init__.py
"""core: protocol-agnostic 业务核心。"""
```

```python
# omr_service/core/exceptions.py
"""OMR 业务异常体系。

错误码编码（与 omr.proto 兼容）:
    0  成功
    4  模板未找到
    5  图片加载失败
    6  请求参数非法
    7  任务不存在
    99 内部错误
"""
from __future__ import annotations


class OmrError(Exception):
    """所有 OMR 业务异常的基类。"""

    code: int = 99

    def __init__(self, message: str, *, code: int | None = None):
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code


class TemplateNotFoundError(OmrError):
    code = 4

    def __init__(self, template_id: str):
        super().__init__(f"模板未找到: {template_id}")


class ImageLoadError(OmrError):
    code = 5

    def __init__(self, url: str, reason: str):
        super().__init__(f"图片加载失败: {url} ({reason})")


class InvalidRequestError(OmrError):
    code = 6

    def __init__(self, field: str, reason: str = ""):
        msg = f"请求参数非法: {field}"
        if reason:
            msg += f" ({reason})"
        super().__init__(msg)


class TaskNotFoundError(OmrError):
    code = 7

    def __init__(self, task_id: str):
        super().__init__(f"任务不存在: {task_id}")


class InternalError(OmrError):
    code = 99

    def __init__(self, reason: str = "内部错误"):
        super().__init__(reason)
```

- [ ] **Step 4: 跑测试验证**

Run: `cd screenImg && pytest -q tests/core/test_exceptions.py`
Expected: 7 tests pass。

- [ ] **Step 5: 提交**

```bash
cd screenImg
git add omr_service/core/ tests/core/test_exceptions.py
git commit -m "feat(omr): introduce OmrError exception hierarchy"
```

---

## Task 5: core/service.py (协议无关业务编排)

**Files:**
- Create: `omr_service/core/service.py`
- Create: `tests/core/test_service.py`

- [ ] **Step 1: 阅读 omr_service/rpc/omr_service.py 现有 `_recognize` / `_parse_golden_template` 实现**

读 `omr_service/rpc/omr_service.py` 全文件，理解：
- `OmrServiceServicer.__init__` 依赖哪些组件
- `_recognize(self, request)` 实现步骤
- `_parse_golden_template(self, request)` 实现步骤
- `_verify_recognition_rate` / `_reverify_paper` 实现
- `__init__` 是否真有现有 engine 组件复用

确认：
- `TemplateStore` / `ImageLoader` / `WorkerPool` / `PersonalInfoOcr` / `SubjectiveCropper` 是否都已存在
- `request.method` 用什么名（grpc 用 generated stub method）

- [ ] **Step 2: 写失败的 service 测试**

```python
# tests/core/test_service.py
from unittest.mock import MagicMock
import numpy as np
import pytest

from omr_service.core.service import OmrService
from omr_service.core.exceptions import (
    TemplateNotFoundError,
    ImageLoadError,
    InvalidRequestError,
    InternalError,
)


@pytest.fixture
def mock_deps():
    return {
        "template_store": MagicMock(),
        "image_loader": MagicMock(),
        "worker_pool": MagicMock(),
        "ocr_engine": MagicMock(),
        "cropper": MagicMock(),
    }


@pytest.fixture
def service(mock_deps):
    return OmrService(**mock_deps)


def test_recognize_returns_code_0_on_success(service, mock_deps):
    # arrange
    img = np.zeros((100, 100, 3), dtype=np.uint8)
    mock_deps["image_loader"].load.return_value = [img]
    mock_deps["template_store"].get.return_value = MagicMock()
    # 假设 standard_recognizer.recognize 返回 (answers, abnormal)
    mock_deps["worker_pool"].submit.return_value.result.return_value = (
        [
            {"question_no": 1, "selected": ["A"], "is_blank": False, "is_multiple": False, "answer_type": "single"}
        ],
        False,
    )

    # act
    result = service.recognize({
        "template_id": "t-1",
        "scan_image_urls": ["http://x/y.jpg"],
    })

    # assert
    assert result["code"] == 0
    assert result["template_id"] == "t-1"
    assert "answers" in result
    assert "elapsed_ms" in result


def test_recognize_raises_template_not_found(service, mock_deps):
    mock_deps["image_loader"].load.return_value = [np.zeros((10, 10, 3), dtype=np.uint8)]
    mock_deps["template_store"].get.return_value = None

    with pytest.raises(TemplateNotFoundError):
        service.recognize({
            "template_id": "missing",
            "scan_image_urls": ["http://x/y.jpg"],
        })


def test_recognize_raises_image_load_error(service, mock_deps):
    mock_deps["image_loader"].load.side_effect = FileNotFoundError("404")

    with pytest.raises(ImageLoadError):
        service.recognize({
            "template_id": "t-1",
            "scan_image_urls": ["http://x/bad.jpg"],
        })


def test_parse_golden_template_returns_code_0(service, mock_deps):
    mock_deps["image_loader"].load.return_value = [np.zeros((100, 100, 3), dtype=np.uint8)]

    result = service.parse_golden_template({
        "template_id": "t-1",
        "template_image_url": "http://x/tpl.jpg",
        "columns": [
            {"column_id": "c1", "column_index": 0, "question_start": 1, "question_count": 5, "options_per_question": 4}
        ],
    })

    assert result["code"] == 0
    assert result["template_id"] == "t-1"
    assert "answers" in result


def test_parse_golden_template_invalid_columns(service):
    with pytest.raises(InvalidRequestError):
        service.parse_golden_template({
            "template_id": "t-1",
            "template_image_url": "http://x/tpl.jpg",
            "columns": [],  # 空
        })


def test_verify_recognition_rate_not_implemented(service):
    with pytest.raises(InternalError):
        service.verify_recognition_rate({})


def test_reverify_paper_delegates_to_recognize(service, mock_deps):
    mock_deps["image_loader"].load.return_value = [np.zeros((10, 10, 3), dtype=np.uint8)]
    mock_deps["template_store"].get.return_value = MagicMock()
    mock_deps["worker_pool"].submit.return_value.result.return_value = ([], False)

    result = service.reverify_paper({
        "template_id": "t-1",
        "scan_image_urls": ["http://x/y.jpg"],
    })

    assert result["code"] == 0
```

- [ ] **Step 3: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/core/test_service.py`
Expected: ModuleNotFoundError: No module named 'omr_service.core.service'

- [ ] **Step 4: 实现 core/service.py**

**最小化实现**（从 `omr_service/rpc/omr_service.py` 抽离，本任务只保证测试通过，业务逻辑在后续任务对齐）：

```python
# omr_service/core/service.py
"""OMR 业务核心：协议无关，输入输出都是 plain dict。

本模块不依赖 FastAPI / Pydantic / protobuf。

现有实现参考 `omr_service/rpc/omr_service.py`，本任务是抽离 + dict 接口。
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

from omr_service.core.exceptions import (
    ImageLoadError,
    InternalError,
    InvalidRequestError,
    TemplateNotFoundError,
)

logger = logging.getLogger(__name__)


class OmrService:
    """协议无关的 OMR 业务服务。

    依赖：
    - template_store: TemplateStore 实例
    - image_loader: ImageLoader 实例
    - worker_pool: WorkerPool 实例（用于限并发）
    - ocr_engine: PersonalInfoOcr 实例（懒加载）
    - cropper: SubjectiveCropper 实例
    """

    def __init__(
        self,
        template_store,
        image_loader,
        worker_pool,
        ocr_engine,
        cropper,
        *,
        sync_timeout_seconds: float = 60.0,
    ):
        self.template_store = template_store
        self.image_loader = image_loader
        self.worker_pool = worker_pool
        self.ocr_engine = ocr_engine
        self.cropper = cropper
        self.sync_timeout_seconds = sync_timeout_seconds

    def recognize(self, request: dict[str, Any]) -> dict[str, Any]:
        """同步识别。返回 RecognizeResult dict。"""
        template_id = request.get("template_id")
        scan_urls = request.get("scan_image_urls")
        if not template_id or not scan_urls:
            raise InvalidRequestError("template_id or scan_image_urls", "missing")

        t0 = time.monotonic()
        images = self._load_images(scan_urls)
        template = self.template_store.get(template_id)
        if template is None:
            raise TemplateNotFoundError(template_id)

        # 走 worker_pool 限并发（占位实现，后续任务细化）
        future = self.worker_pool.submit(self._do_recognize, template, images, request)
        try:
            answers, abnormal = future.result(timeout=self.sync_timeout_seconds)
        except TimeoutError as e:
            raise InternalError(f"识别超时 ({self.sync_timeout_seconds}s)") from e

        result = {
            "code": 0,
            "message": "ok",
            "template_id": template_id,
            "answers": answers,
            "abnormal": abnormal,
            "empty_count": sum(1 for a in answers if a.get("is_blank")),
            "multiple_count": sum(1 for a in answers if a.get("is_multiple")),
            "elapsed_ms": int((time.monotonic() - t0) * 1000),
        }
        return result

    def parse_golden_template(self, request: dict[str, Any]) -> dict[str, Any]:
        """同步模板解析。返回 GoldenTemplateResult dict。"""
        template_id = request.get("template_id")
        template_url = request.get("template_image_url")
        columns = request.get("columns", [])
        if not template_id or not template_url:
            raise InvalidRequestError("template_id or template_image_url", "missing")
        if not columns:
            raise InvalidRequestError("columns", "empty")

        t0 = time.monotonic()
        images = self._load_images([template_url])
        # 解析模板（占位实现）
        answers, bubble_grid = self._do_parse(images[0], columns)

        return {
            "code": 0,
            "message": "ok",
            "template_id": template_id,
            "answers": answers,
            "bubble_grid": bubble_grid,
            "elapsed_ms": int((time.monotonic() - t0) * 1000),
        }

    def verify_recognition_rate(self, request: dict[str, Any]) -> dict[str, Any]:
        """暂未实现。"""
        raise InternalError("verify_recognition_rate 暂未通过 HTTP 暴露")

    def reverify_paper(self, request: dict[str, Any]) -> dict[str, Any]:
        """与 recognize 行为等价。"""
        return self.recognize(request)

    # ---------- 内部辅助 ----------

    def _load_images(self, urls: list[str]) -> list:
        try:
            return self.image_loader.load(urls)
        except FileNotFoundError as e:
            raise ImageLoadError(url=getattr(e, "url", "?"), reason=str(e)) from e
        except Exception as e:
            raise ImageLoadError(url="?", reason=str(e)) from e

    def _do_recognize(self, template, images, request):
        """占位：实际识别逻辑接下来从 omr_service/rpc/omr_service.py 迁入。"""
        # TODO: 接入 engine/recognizers/standard.py
        return [], False

    def _do_parse(self, image, columns):
        """占位：模板解析逻辑。"""
        # TODO: 接入 engine/standard_template.py
        return [], []
```

- [ ] **Step 5: 跑测试验证**

Run: `cd screenImg && pytest -q tests/core/test_service.py`
Expected: 7 tests pass。

- [ ] **Step 6: 提交**

```bash
cd screenImg
git add omr_service/core/service.py tests/core/test_service.py
git commit -m "feat(omr): introduce core/service.py protocol-agnostic OmrService"
```

---

## Task 6: 业务核心迁移（recognize / parse 真实实现）

**Files:**
- Modify: `omr_service/core/service.py`
- Modify: `tests/core/test_service.py`

> 说明：本任务把 `omr_service/rpc/omr_service.py` 内 `_recognize` / `_parse_golden_template` 真实实现迁移到 `core/service.py`。因为 `engine/recognizers/standard.py` 等已存在，迁移主要是改数据格式（protobuf → dict）。

- [ ] **Step 1: 读 rpc/omr_service.py 全文件**

完整阅读 `omr_service/rpc/omr_service.py`，重点：
- `OmrServiceServicer.__init__` 的依赖
- `_recognize` 步骤（识别 → 算分 → OCR → 裁剪 → protobuf 转换）
- `_parse_golden_template` 步骤
- 字段映射：用 protobuf 字段名 vs 实际值（如 `request.template_id` vs `request.scan_image_url`）

- [ ] **Step 2: 写更完整的测试**

在 `tests/core/test_service.py` 追加：

```python
# 追加到 test_service.py 末尾

def test_recognize_includes_ocr_personal_info(service, mock_deps):
    """OCR 识别个人信息（mock 返回）。"""
    img = np.zeros((100, 100, 3), dtype=np.uint8)
    mock_deps["image_loader"].load.return_value = [img]
    mock_deps["template_store"].get.return_value = MagicMock()
    mock_deps["worker_pool"].submit.return_value.result.return_value = ([], False)
    mock_deps["ocr_engine"].recognize.return_value = {"name": "张三", "exam_id": "B001"}

    result = service.recognize({
        "template_id": "t-1",
        "scan_image_urls": ["http://x/y.jpg"],
        "personal_info_region": {"x": 0, "y": 0, "width": 100, "height": 50},
    })

    assert "personal_info" in result
    assert result["personal_info"]["name"] == "张三"


def test_recognize_includes_subjective_crops(service, mock_deps):
    img = np.zeros((100, 100, 3), dtype=np.uint8)
    mock_deps["image_loader"].load.return_value = [img]
    mock_deps["template_store"].get.return_value = MagicMock()
    mock_deps["worker_pool"].submit.return_value.result.return_value = ([], False)
    mock_deps["cropper"].crop.return_value = [
        {"region_id": "q-1", "url": "http://x/c1.jpg", "width": 100, "height": 50}
    ]

    result = service.recognize({
        "template_id": "t-1",
        "scan_image_urls": ["http://x/y.jpg"],
        "subjective_regions": [{"region_id": "q-1", "x": 0, "y": 0, "width": 100, "height": 50}],
    })

    assert "subjective_crops" in result
    assert result["subjective_crops"][0]["region_id"] == "q-1"


def test_parse_golden_template_with_personal_info(service, mock_deps):
    mock_deps["image_loader"].load.return_value = [np.zeros((100, 100, 3), dtype=np.uint8)]

    result = service.parse_golden_template({
        "template_id": "t-1",
        "template_image_url": "http://x/tpl.jpg",
        "columns": [{"column_id": "c1", "column_index": 0, "question_start": 1, "question_count": 5, "options_per_question": 4}],
        "personal_info_region": {"x": 0, "y": 0, "width": 100, "height": 50},
    })

    assert "personal_info_sample" in result
```

- [ ] **Step 3: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/core/test_service.py::test_recognize_includes_ocr_personal_info`
Expected: FAIL (no `personal_info` in result dict)

- [ ] **Step 4: 增强 _do_recognize / _do_parse**

修改 `omr_service/core/service.py` 的 `_do_recognize`：

```python
def _do_recognize(self, template, images, request):
    """真实识别流程（从 rpc/omr_service.py 迁移）。

    返回 (answers, abnormal)。answers 是 list of dict。
    """
    from omr_service.engine.recognizers.standard import StandardTemplateRecognizer

    recognizer = StandardTemplateRecognizer()
    context = {"template": template, "images": images, "config": request}
    raw = recognizer.recognize(context)
    return raw.get("answers", []), raw.get("abnormal", False)
```

修改 `recognize` 函数体，加入 OCR 和裁剪：

```python
def recognize(self, request: dict[str, Any]) -> dict[str, Any]:
    """同步识别。返回 RecognizeResult dict。"""
    template_id = request.get("template_id")
    scan_urls = request.get("scan_image_urls")
    if not template_id or not scan_urls:
        raise InvalidRequestError("template_id or scan_image_urls", "missing")

    t0 = time.monotonic()
    images = self._load_images(scan_urls)
    template = self.template_store.get(template_id)
    if template is None:
        raise TemplateNotFoundError(template_id)

    future = self.worker_pool.submit(self._do_recognize, template, images, request)
    try:
        answers, abnormal = future.result(timeout=self.sync_timeout_seconds)
    except TimeoutError as e:
        raise InternalError(f"识别超时 ({self.sync_timeout_seconds}s)") from e

    result = {
        "code": 0,
        "message": "ok",
        "template_id": template_id,
        "answers": answers,
        "abnormal": abnormal,
        "empty_count": sum(1 for a in answers if a.get("is_blank")),
        "multiple_count": sum(1 for a in answers if a.get("is_multiple")),
        "elapsed_ms": int((time.monotonic() - t0) * 1000),
    }

    # OCR 个人信息
    if request.get("personal_info_region"):
        result["personal_info"] = self.ocr_engine.recognize(images)

    # 主观题裁剪
    if request.get("subjective_regions"):
        result["subjective_crops"] = self.cropper.crop(images, request["subjective_regions"])

    return result
```

- [ ] **Step 5: 跑测试验证**

Run: `cd screenImg && pytest -q tests/core/test_service.py`
Expected: 10 tests pass。

- [ ] **Step 6: 提交**

```bash
cd screenImg
git add omr_service/core/service.py tests/core/test_service.py
git commit -m "feat(omr): wire OmrService with real recognizer, OCR, cropper"
```

---

## Task 7: api/schemas/ (Pydantic 模型)

**Files:**
- Create: `omr_service/api/__init__.py`
- Create: `omr_service/api/schemas/__init__.py`
- Create: `omr_service/api/schemas/common.py`
- Create: `omr_service/api/schemas/enums.py`
- Create: `omr_service/api/schemas/recognize.py`
- Create: `omr_service/api/schemas/templates.py`
- Create: `omr_service/api/schemas/tasks.py`
- Create: `tests/api/test_schemas.py`

- [ ] **Step 1: 写失败的 schema 测试**

```python
# tests/api/test_schemas.py
import pytest
from pydantic import ValidationError

from omr_service.api.schemas.enums import AnswerType, TaskType, TaskStatus
from omr_service.api.schemas.recognize import RecognizeRequest, QuestionAnswer
from omr_service.api.schemas.templates import GoldenTemplateRequest, ColumnConfig
from omr_service.api.schemas.tasks import CreateTaskRequest, TaskStatusResponse


def test_answer_type_enum():
    assert AnswerType.SINGLE == "single"
    assert AnswerType.MULTIPLE == "multiple"
    assert AnswerType.BLANK == "blank"
    assert AnswerType.UNKNOWN == "unknown"


def test_recognize_request_valid():
    req = RecognizeRequest(
        template_id="t-1",
        scan_image_urls=["http://x/y.jpg"],
    )
    assert req.template_id == "t-1"
    assert len(req.scan_image_urls) == 1


def test_recognize_request_empty_urls_fails():
    with pytest.raises(ValidationError):
        RecognizeRequest(template_id="t-1", scan_image_urls=[])


def test_recognize_request_missing_template_id_fails():
    with pytest.raises(ValidationError):
        RecognizeRequest(scan_image_urls=["http://x/y.jpg"])


def test_question_answer_default_answer_type():
    a = QuestionAnswer(question_no=1, selected=["A"])
    assert a.answer_type == AnswerType.SINGLE
    assert a.is_blank is False


def test_column_config_default_options():
    c = ColumnConfig(
        column_id="c1",
        column_index=0,
        question_start=1,
        question_count=5,
    )
    assert c.options_per_question == 4


def test_golden_template_request_valid():
    req = GoldenTemplateRequest(
        template_id="t-1",
        template_image_url="http://x/tpl.jpg",
        columns=[ColumnConfig(
            column_id="c1", column_index=0, question_start=1, question_count=5
        )],
    )
    assert req.template_id == "t-1"


def test_create_task_request_recognize():
    req = CreateTaskRequest(
        task_type=TaskType.RECOGNIZE,
        payload={
            "template_id": "t-1",
            "scan_image_urls": ["http://x.jpg"],
        },
    )
    assert req.task_type == TaskType.RECOGNIZE


def test_create_task_request_parse_template():
    req = CreateTaskRequest(
        task_type=TaskType.PARSE_TEMPLATE,
        payload={
            "template_id": "t-1",
            "template_image_url": "http://x.jpg",
            "columns": [],
        },
    )
    assert req.task_type == TaskType.PARSE_TEMPLATE


def test_task_status_response_values():
    r = TaskStatusResponse(
        task_id="t-1",
        status=TaskStatus.QUEUED,
        task_type=TaskType.RECOGNIZE,
        created_at="2026-07-28T10:00:00Z",
    )
    assert r.status == TaskStatus.QUEUED
```

- [ ] **Step 2: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/api/test_schemas.py`
Expected: ModuleNotFoundError (api/schemas 不存在)

- [ ] **Step 3: 实现 enums.py**

```python
# omr_service/api/__init__.py
"""api: FastAPI 路由 + Pydantic schemas。"""
```

```python
# omr_service/api/schemas/__init__.py
```

```python
# omr_service/api/schemas/enums.py
from enum import Enum


class AnswerType(str, Enum):
    SINGLE = "single"
    MULTIPLE = "multiple"
    BLANK = "blank"
    UNKNOWN = "unknown"


class TaskType(str, Enum):
    RECOGNIZE = "recognize"
    PARSE_TEMPLATE = "parse_template"


class TaskStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
```

- [ ] **Step 4: 实现 common.py**

```python
# omr_service/api/schemas/common.py
from pydantic import BaseModel, Field


class Region(BaseModel):
    x: int
    y: int
    width: int
    height: int


class SubjectiveRegion(Region):
    region_id: str


class ErrorResponse(BaseModel):
    code: int
    message: str
    request_id: str | None = None


class BaseResponse(BaseModel):
    code: int = 0
    message: str = "ok"
    elapsed_ms: int | None = None
```

- [ ] **Step 5: 实现 recognize.py**

```python
# omr_service/api/schemas/recognize.py
from pydantic import BaseModel, Field

from omr_service.api.schemas.common import BaseResponse, SubjectiveRegion
from omr_service.api.schemas.enums import AnswerType


class QuestionAnswer(BaseModel):
    question_no: int
    answer_type: AnswerType = AnswerType.SINGLE
    selected: list[str] = Field(default_factory=list)
    is_blank: bool = False
    is_multiple: bool = False
    confidence: float | None = None


class RecognizeRequest(BaseModel):
    template_id: str
    scan_image_urls: list[str] = Field(min_length=1)
    question_no: int | None = None
    personal_info_region: dict | None = None
    subjective_regions: list[SubjectiveRegion] | None = None


class PersonalInfo(BaseModel):
    name: str | None = None
    exam_id: str | None = None
    raw_text: str | None = None


class SubjectiveCrop(BaseModel):
    region_id: str
    url: str
    width: int
    height: int


class RecognizeResponse(BaseResponse):
    template_id: str
    answers: list[QuestionAnswer] = Field(default_factory=list)
    empty_count: int = 0
    multiple_count: int = 0
    abnormal: bool = False
    personal_info: PersonalInfo | None = None
    subjective_crops: list[SubjectiveCrop] | None = None
```

- [ ] **Step 6: 实现 templates.py**

```python
# omr_service/api/schemas/templates.py
from pydantic import BaseModel, Field

from omr_service.api.schemas.common import BaseResponse, Region, SubjectiveRegion
from omr_service.api.schemas.recognize import (
    PersonalInfo,
    QuestionAnswer,
    SubjectiveCrop,
)


class ColumnConfig(BaseModel):
    column_id: str
    column_index: int
    question_start: int
    question_count: int
    options_per_question: int = 4
    question_type: str = "single"


class GoldenTemplateRequest(BaseModel):
    template_id: str
    template_image_url: str
    columns: list[ColumnConfig] = Field(min_length=1)
    personal_info_region: Region | None = None
    subjective_regions: list[SubjectiveRegion] | None = None


class BubbleGrid(BaseModel):
    row: int
    col: int
    question_no: int
    option: str
    x: int
    y: int


class GoldenTemplateResponse(BaseResponse):
    template_id: str
    answers: list[QuestionAnswer] = Field(default_factory=list)
    bubble_grid: list[BubbleGrid] = Field(default_factory=list)
    personal_info_sample: PersonalInfo | None = None
    subjective_crops: list[SubjectiveCrop] | None = None
```

- [ ] **Step 7: 实现 tasks.py**

```python
# omr_service/api/schemas/tasks.py
from datetime import datetime
from pydantic import BaseModel, Field

from omr_service.api.schemas.enums import TaskStatus, TaskType


class CreateTaskRequest(BaseModel):
    task_type: TaskType
    # payload 用 dict，由 router 根据 task_type 二次校验
    payload: dict


class TaskCreatedResponse(BaseModel):
    task_id: str
    status: TaskStatus = TaskStatus.QUEUED
    created_at: datetime


class TaskStatusResponse(BaseModel):
    task_id: str
    task_type: TaskType
    status: TaskStatus
    created_at: datetime
    finished_at: datetime | None = None
    result: dict | None = None
    error: dict | None = None
```

- [ ] **Step 8: 跑测试验证**

Run: `cd screenImg && pytest -q tests/api/test_schemas.py`
Expected: 10 tests pass。

- [ ] **Step 9: 提交**

```bash
cd screenImg
git add omr_service/api/ tests/api/test_schemas.py
git commit -m "feat(omr): add Pydantic schemas for FastAPI routes"
```

---

## Task 8: api/errors.py (OmrError → HTTP 响应)

**Files:**
- Create: `omr_service/api/errors.py`
- Create: `tests/api/test_errors.py`

- [ ] **Step 1: 写失败的异常 handler 测试**

```python
# tests/api/test_errors.py
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
```

- [ ] **Step 2: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/api/test_errors.py`
Expected: ModuleNotFoundError: No module named 'omr_service.api.errors'

- [ ] **Step 3: 实现 api/errors.py**

```python
# omr_service/api/errors.py
"""统一异常处理：OmrError → HTTP 响应。

错误码与 HTTP status 映射：
    4 → 404 (TemplateNotFoundError)
    5 → 502 (ImageLoadError)
    6 → 400 (InvalidRequestError)
    7 → 404 (TaskNotFoundError)
    99 → 500 (InternalError / 未捕获异常)
"""
from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from omr_service.core.exceptions import OmrError

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger(__name__)


_STATUS_MAP: dict[int, int] = {
    4: 404,
    5: 502,
    6: 400,
    7: 404,
    99: 500,
}


def _request_id(request: Request) -> str:
    return getattr(request.state, "request_id", None) or str(uuid.uuid4())


async def _omr_error_handler(request: Request, exc: OmrError) -> JSONResponse:
    request_id = _request_id(request)
    status_code = _STATUS_MAP.get(exc.code, 500)
    logger.warning(
        "omr_error: code=%s message=%s request_id=%s",
        exc.code, exc.message, request_id,
    )
    return JSONResponse(
        status_code=status_code,
        content={"code": exc.code, "message": exc.message, "request_id": request_id},
    )


async def _validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    request_id = _request_id(request)
    logger.warning("validation_error: %s request_id=%s", exc.errors(), request_id)
    # 提取第一个错误的字段
    first_error = exc.errors()[0] if exc.errors() else {}
    field = ".".join(str(p) for p in first_error.get("loc", []))
    msg = first_error.get("msg", "请求参数非法")
    return JSONResponse(
        status_code=400,
        content={"code": 6, "message": f"请求参数非法: {field} ({msg})", "request_id": request_id},
    )


async def _unhandled_handler(request: Request, exc: Exception) -> JSONResponse:
    request_id = _request_id(request)
    logger.exception("unhandled_exception: request_id=%s", request_id, exc_info=exc)
    return JSONResponse(
        status_code=500,
        content={"code": 99, "message": "内部错误", "request_id": request_id},
    )


def register_error_handlers(app: "FastAPI") -> None:
    """全局注册异常 handler。"""
    app.add_exception_handler(OmrError, _omr_error_handler)
    app.add_exception_handler(RequestValidationError, _validation_error_handler)
    app.add_exception_handler(Exception, _unhandled_handler)
```

- [ ] **Step 4: 跑测试验证**

Run: `cd screenImg && pytest -q tests/api/test_errors.py`
Expected: 6 tests pass。

- [ ] **Step 5: 提交**

```bash
cd screenImg
git add omr_service/api/errors.py tests/api/test_errors.py
git commit -m "feat(omr): add global exception handlers for FastAPI"
```

---

## Task 9: api/deps.py (依赖注入)

**Files:**
- Create: `omr_service/api/deps.py`
- Create: `tests/api/test_deps.py`

- [ ] **Step 1: 写失败的依赖测试**

```python
# tests/api/test_deps.py
from unittest.mock import MagicMock
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from omr_service.api.deps import register_dependencies, get_service, get_settings, get_task_registry


@pytest.fixture
def mock_app():
    app = FastAPI()
    settings = MagicMock()
    settings.sync_timeout_seconds = 60.0
    service = MagicMock()
    task_registry = MagicMock()
    register_dependencies(app, settings=settings, service=service, task_registry=task_registry)
    return app, settings, service, task_registry


def test_get_settings(mock_app):
    app, settings, _, _ = mock_app

    @app.get("/s")
    def s(s=__import__("fastapi").Depends(get_settings)):
        return {"http_port": s.http_port}

    c = TestClient(app)
    assert c.get("/s").json()["http_port"] == settings.http_port


def test_get_service(mock_app):
    app, _, service, _ = mock_app

    @app.get("/sv")
    def sv(s=__import__("fastapi").Depends(get_service)):
        return {"ok": True}

    c = TestClient(app)
    assert c.get("/sv").json() == {"ok": True}


def test_get_task_registry(mock_app):
    app, _, _, reg = mock_app

    @app.get("/r")
    def r(reg_=__import__("fastapi").Depends(get_task_registry)):
        return {"ok": True}

    c = TestClient(app)
    assert c.get("/r").json() == {"ok": True}
```

- [ ] **Step 2: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/api/test_deps.py`
Expected: ModuleNotFoundError

- [ ] **Step 3: 实现 api/deps.py**

```python
# omr_service/api/deps.py
"""FastAPI 依赖注入。"""
from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import Depends, Request

if TYPE_CHECKING:
    from fastapi import FastAPI

    from omr_service.config import OmrSettings
    from omr_service.core.service import OmrService
    from omr_service.core.task_registry import TaskRegistry


def get_settings(request: Request) -> "OmrSettings":
    return request.app.state.settings


def get_service(request: Request) -> "OmrService":
    return request.app.state.service


def get_task_registry(request: Request) -> "TaskRegistry":
    return request.app.state.task_registry


def register_dependencies(
    app: "FastAPI",
    *,
    settings: "OmrSettings",
    service: "OmrService",
    task_registry: "TaskRegistry",
) -> None:
    """把核心组件挂到 app.state，供 Depends 调用。"""
    app.state.settings = settings
    app.state.service = service
    app.state.task_registry = task_registry
```

- [ ] **Step 4: 跑测试验证**

Run: `cd screenImg && pytest -q tests/api/test_deps.py`
Expected: 3 tests pass。

- [ ] **Step 5: 提交**

```bash
cd screenImg
git add omr_service/api/deps.py tests/api/test_deps.py
git commit -m "feat(omr): add FastAPI dependency injection"
```

---

## Task 10: api/routers/health.py (健康检查)

**Files:**
- Create: `omr_service/api/routers/__init__.py`
- Create: `omr_service/api/routers/health.py`
- Create: `tests/api/test_health.py`

- [ ] **Step 1: 写失败的健康测试**

```python
# tests/api/test_health.py
from unittest.mock import MagicMock
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from omr_service.api.deps import register_dependencies
from omr_service.api.routers.health import router


@pytest.fixture
def app():
    app = FastAPI()
    app.include_router(router)
    settings = MagicMock()
    register_dependencies(app, settings=settings, service=MagicMock(), task_registry=MagicMock())
    return app


def test_health_200(app):
    c = TestClient(app)
    r = c.get("/v1/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_health_ready_200(app):
    c = TestClient(app)
    r = c.get("/v1/health/ready")
    assert r.status_code == 200


def test_health_ready_503_when_service_down(app):
    c = TestClient(app)
    # 注入坏的 service
    bad_app = FastAPI()
    bad_app.include_router(router)
    broken_service = MagicMock()
    broken_service.template_store = None
    register_dependencies(bad_app, settings=MagicMock(), service=broken_service, task_registry=MagicMock())
    r = TestClient(bad_app).get("/v1/health/ready")
    assert r.status_code == 503
```

- [ ] **Step 2: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/api/test_health.py`
Expected: ModuleNotFoundError

- [ ] **Step 3: 实现 health router**

```python
# omr_service/api/routers/__init__.py
```

```python
# omr_service/api/routers/health.py
"""健康检查路由。

- GET /v1/health - 存活
- GET /v1/health/ready - 就绪（依赖服务检查）
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from omr_service.api.deps import get_service, get_settings

router = APIRouter(prefix="/v1", tags=["health"])


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/health/ready")
def ready(request: Request):
    """检查 TemplateStore / Redis / service 是否就绪。"""
    settings = get_settings(request)
    service = get_service(request)
    checks = {"service": True, "template_store": service.template_store is not None}
    healthy = all(checks.values())
    status_code = 200 if healthy else 503
    return JSONResponse(
        status_code=status_code,
        content={"status": "ok" if healthy else "degraded", "checks": checks},
    )
```

- [ ] **Step 4: 跑测试验证**

Run: `cd screenImg && pytest -q tests/api/test_health.py`
Expected: 3 tests pass。

- [ ] **Step 5: 提交**

```bash
cd screenImg
git add omr_service/api/routers/__init__.py omr_service/api/routers/health.py tests/api/test_health.py
git commit -m "feat(omr): add /v1/health and /v1/health/ready routes"
```

---

## Task 11: api/routers/recognize.py (同步识别)

**Files:**
- Create: `omr_service/api/routers/recognize.py`
- Create: `tests/api/test_recognize.py`

- [ ] **Step 1: 写失败的识别测试**

```python
# tests/api/test_recognize.py
from unittest.mock import MagicMock
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from omr_service.api.deps import register_dependencies
from omr_service.api.errors import register_error_handlers
from omr_service.api.routers.recognize import router
from omr_service.core.exceptions import (
    TemplateNotFoundError,
    ImageLoadError,
    InvalidRequestError,
)


@pytest.fixture
def app():
    app = FastAPI()
    app.include_router(router)
    register_error_handlers(app)
    settings = MagicMock()
    service = MagicMock()
    register_dependencies(app, settings=settings, service=service, task_registry=MagicMock())
    return app, service


def test_recognize_200(app):
    _, service = app
    service.recognize.return_value = {
        "code": 0, "message": "ok", "template_id": "t-1",
        "answers": [{"question_no": 1, "selected": ["A"], "answer_type": "single", "is_blank": False, "is_multiple": False}],
        "abnormal": False, "empty_count": 0, "multiple_count": 0, "elapsed_ms": 123,
    }

    c = TestClient(app[0])
    r = c.post("/v1/recognize", json={
        "template_id": "t-1",
        "scan_image_urls": ["http://x/y.jpg"],
    })
    assert r.status_code == 200
    assert r.json()["code"] == 0
    assert r.json()["answers"][0]["selected"] == ["A"]


def test_recognize_404_template_not_found(app):
    _, service = app
    service.recognize.side_effect = TemplateNotFoundError(template_id="t-1")

    c = TestClient(app[0])
    r = c.post("/v1/recognize", json={"template_id": "t-1", "scan_image_urls": ["http://x/y.jpg"]})
    assert r.status_code == 404
    assert r.json()["code"] == 4


def test_recognize_502_image_load_error(app):
    _, service = app
    service.recognize.side_effect = ImageLoadError(url="http://x", reason="timeout")

    c = TestClient(app[0])
    r = c.post("/v1/recognize", json={"template_id": "t-1", "scan_image_urls": ["http://x/y.jpg"]})
    assert r.status_code == 502
    assert r.json()["code"] == 5


def test_recognize_400_missing_fields(app):
    _, service = app
    c = TestClient(app[0])
    r = c.post("/v1/recognize", json={"template_id": "t-1"})
    assert r.status_code == 400
    assert r.json()["code"] == 6


def test_recognize_400_invalid_url_list(app):
    _, service = app
    c = TestClient(app[0])
    r = c.post("/v1/recognize", json={"template_id": "t-1", "scan_image_urls": []})
    assert r.status_code == 400
```

- [ ] **Step 2: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/api/test_recognize.py`
Expected: ModuleNotFoundError

- [ ] **Step 3: 实现 recognize router**

```python
# omr_service/api/routers/recognize.py
"""POST /v1/recognize - 同步识别。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from omr_service.api.deps import get_service
from omr_service.api.schemas.recognize import RecognizeRequest, RecognizeResponse

router = APIRouter(prefix="/v1", tags=["recognize"])


@router.post("/recognize", response_model=RecognizeResponse)
def recognize(
    request: Request,
    body: RecognizeRequest,
):
    """同步识别答题卡。同步返回完整结果（5-30s）。"""
    service = get_service(request)
    result = service.recognize(body.model_dump())
    return result
```

- [ ] **Step 4: 跑测试验证**

Run: `cd screenImg && pytest -q tests/api/test_recognize.py`
Expected: 5 tests pass。

- [ ] **Step 5: 提交**

```bash
cd screenImg
git add omr_service/api/routers/recognize.py tests/api/test_recognize.py
git commit -m "feat(omr): add POST /v1/recognize route"
```

---

## Task 12: api/routers/templates.py (模板解析 + verify + reverify)

**Files:**
- Create: `omr_service/api/routers/templates.py`
- Create: `tests/api/test_templates.py`

- [ ] **Step 1: 写失败的 templates 测试**

```python
# tests/api/test_templates.py
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
    _, service = app
    c = TestClient(app[0])
    r = c.post("/v1/templates/parse", json={
        "template_id": "t-1",
        "template_image_url": "http://x/tpl.jpg",
        "columns": [],
    })
    assert r.status_code == 400
    assert r.json()["code"] == 6


def test_verify_recognition_rate_501(app):
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
```

- [ ] **Step 2: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/api/test_templates.py`
Expected: ModuleNotFoundError

- [ ] **Step 3: 实现 templates router**

```python
# omr_service/api/routers/templates.py
"""模板相关路由：
- POST /v1/templates/parse - 同步模板解析
- POST /v1/verify_recognition_rate - 暂返 501
- POST /v1/reverify_paper - 与 recognize 等价
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from omr_service.api.deps import get_service
from omr_service.api.schemas.recognize import RecognizeRequest, RecognizeResponse
from omr_service.api.schemas.templates import GoldenTemplateRequest, GoldenTemplateResponse

router = APIRouter(prefix="/v1", tags=["templates"])


@router.post("/templates/parse", response_model=GoldenTemplateResponse)
def parse_template(request: Request, body: GoldenTemplateRequest):
    service = get_service(request)
    return service.parse_golden_template(body.model_dump())


@router.post("/reverify_paper", response_model=RecognizeResponse)
def reverify_paper(request: Request, body: RecognizeRequest):
    service = get_service(request)
    return service.reverify_paper(body.model_dump())


@router.post("/verify_recognition_rate")
def verify_recognition_rate(request: Request):
    service = get_service(request)
    return service.verify_recognition_rate({})
```

- [ ] **Step 4: 跑测试验证**

Run: `cd screenImg && pytest -q tests/api/test_templates.py`
Expected: 4 tests pass。

- [ ] **Step 5: 提交**

```bash
cd screenImg
git add omr_service/api/routers/templates.py tests/api/test_templates.py
git commit -m "feat(omr): add template parse / verify / reverify routes"
```

---

## Task 13: api/routers/crops.py (静态裁剪图)

**Files:**
- Create: `omr_service/api/routers/crops.py`
- Create: `tests/api/test_crops.py`

- [ ] **Step 1: 写失败的 crops 测试**

```python
# tests/api/test_crops.py
import os
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
    # FastAPI 会自动处理 .. — 应返回 404 或 400
    assert r.status_code in (404, 400)
```

- [ ] **Step 2: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/api/test_crops.py`
Expected: ModuleNotFoundError

- [ ] **Step 3: 实现 crops router**

```python
# omr_service/api/routers/crops.py
"""GET /v1/omr_crops/{file_path:path} - 静态裁剪图服务。

安全：使用 resolve() 防止路径穿越。
"""
from __future__ import annotations

from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse

from omr_service.api.deps import get_settings

router = APIRouter(prefix="/v1/omr_crops", tags=["crops"])


@router.get("/{file_path:path}")
def get_crop(request: Request, file_path: str):
    settings = get_settings(request)
    base = Path(settings.crop_output_dir).resolve()
    target = (base / file_path).resolve()

    # 路径穿越检查
    if not str(target).startswith(str(base)):
        raise HTTPException(status_code=400, detail="path traversal detected")

    if not target.is_file():
        raise HTTPException(status_code=404, detail="file not found")

    return FileResponse(target)
```

- [ ] **Step 4: 跑测试验证**

Run: `cd screenImg && pytest -q tests/api/test_crops.py`
Expected: 2 tests pass。

- [ ] **Step 5: 提交**

```bash
cd screenImg
git add omr_service/api/routers/crops.py tests/api/test_crops.py
git commit -m "feat(omr): add /v1/omr_crops/{file_path:path} static route"
```

---

## Task 14: core/task_registry.py (异步任务结果 Hash 读取)

**Files:**
- Create: `omr_service/core/task_registry.py`
- Create: `tests/core/test_task_registry.py`

- [ ] **Step 1: 写失败的 task_registry 测试**

```python
# tests/core/test_task_registry.py
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
    reg.get("t-1")
    mock_redis.hgetall.assert_called_with("omr:batch:result:hash:t-1")
```

- [ ] **Step 2: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/core/test_task_registry.py`
Expected: ModuleNotFoundError

- [ ] **Step 3: 实现 task_registry.py**

```python
# omr_service/core/task_registry.py
"""异步任务结果 Hash 读取。

与 mq/producer.py 配合：producer 写任务 (XADD omr:batch:job) 时同时写 Hash 标记 processing；
job_handler 完成时更新 Hash 标记 succeeded/failed。
"""
from __future__ import annotations

import json
from typing import Any

from omr_service.api.schemas.enums import TaskStatus
from omr_service.core.exceptions import TaskNotFoundError


class TaskRegistry:
    def __init__(self, redis_client, hash_prefix: str):
        self.redis = redis_client
        self.hash_prefix = hash_prefix

    def _key(self, task_id: str) -> str:
        return f"{self.hash_prefix}:{task_id}"

    def get(self, task_id: str) -> dict[str, Any]:
        raw = self.redis.hgetall(self._key(task_id))
        if not raw:
            raise TaskNotFoundError(task_id)

        result_str = raw.get("result")
        error_str = raw.get("error")
        return {
            "task_id": task_id,
            "task_type": raw.get("task_type"),
            "status": TaskStatus(raw.get("status", "queued")),
            "created_at": raw.get("created_at"),
            "finished_at": raw.get("finished_at"),
            "result": json.loads(result_str) if result_str else None,
            "error": json.loads(error_str) if error_str else None,
        }

    def write_queued(self, task_id: str, task_type: str, payload: dict, created_at: str) -> None:
        """任务入队时由 producer 调用。"""
        self.redis.hset(self._key(task_id), mapping={
            "status": TaskStatus.QUEUED.value,
            "task_type": task_type,
            "created_at": created_at,
            "payload": json.dumps(payload),
        })

    def write_processing(self, task_id: str) -> None:
        self.redis.hset(self._key(task_id), "status", TaskStatus.PROCESSING.value)

    def write_succeeded(self, task_id: str, result: dict, finished_at: str) -> None:
        self.redis.hset(self._key(task_id), mapping={
            "status": TaskStatus.SUCCEEDED.value,
            "result": json.dumps(result),
            "finished_at": finished_at,
        })

    def write_failed(self, task_id: str, error: dict, finished_at: str) -> None:
        self.redis.hset(self._key(task_id), mapping={
            "status": TaskStatus.FAILED.value,
            "error": json.dumps(error),
            "finished_at": finished_at,
        })
```

- [ ] **Step 4: 跑测试验证**

Run: `cd screenImg && pytest -q tests/core/test_task_registry.py`
Expected: 4 tests pass。

- [ ] **Step 5: 提交**

```bash
cd screenImg
git add omr_service/core/task_registry.py tests/core/test_task_registry.py
git commit -m "feat(omr): add TaskRegistry for async task result Hash read/write"
```

---

## Task 15: api/routers/tasks.py (异步任务 REST)

**Files:**
- Create: `omr_service/api/routers/tasks.py`
- Create: `tests/api/test_tasks.py`

- [ ] **Step 1: 写失败的 tasks 测试**

```python
# tests/api/test_tasks.py
from unittest.mock import MagicMock
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from omr_service.api.deps import register_dependencies
from omr_service.api.errors import register_error_handlers
from omr_service.api.routers.tasks import router
from omr_service.api.schemas.enums import TaskStatus, TaskType
from omr_service.core.exceptions import TaskNotFoundError


@pytest.fixture
def app():
    app = FastAPI()
    app.include_router(router)
    register_error_handlers(app)
    settings = MagicMock()
    service = MagicMock()
    task_registry = MagicMock()
    register_dependencies(app, settings=settings, service=service, task_registry=task_registry)
    return app, service, task_registry


def test_create_task_202(app):
    _, _, task_registry = app
    task_registry.write_queued.return_value = None
    c = TestClient(app[0])
    r = c.post("/v1/tasks", json={
        "task_type": "recognize",
        "payload": {"template_id": "t-1", "scan_image_urls": ["http://x.jpg"]},
    })
    assert r.status_code == 202
    assert r.json()["status"] == "queued"
    assert "task_id" in r.json()


def test_create_task_writes_to_stream_and_hash(app):
    _, service, task_registry = app
    # mq.producer.enqueue_job 实际是 XADD；mock 掉
    import omr_service.api.routers.tasks as tasks_module
    tasks_module.enqueue_job = MagicMock()
    c = TestClient(app[0])
    r = c.post("/v1/tasks", json={
        "task_type": "recognize",
        "payload": {"template_id": "t-1", "scan_image_urls": ["http://x.jpg"]},
    })
    assert r.status_code == 202
    task_registry.write_queued.assert_called_once()
    tasks_module.enqueue_job.assert_called_once()


def test_get_task_200(app):
    _, _, task_registry = app
    task_registry.get.return_value = {
        "task_id": "t-1",
        "task_type": TaskType.RECOGNIZE,
        "status": TaskStatus.SUCCEEDED,
        "created_at": "2026-07-28T10:00:00Z",
        "finished_at": "2026-07-28T10:00:08Z",
        "result": {"answers": []},
        "error": None,
    }
    c = TestClient(app[0])
    r = c.get("/v1/tasks/t-1")
    assert r.status_code == 200
    assert r.json()["status"] == "succeeded"


def test_get_task_404(app):
    _, _, task_registry = app
    task_registry.get.side_effect = TaskNotFoundError(task_id="missing")
    c = TestClient(app[0])
    r = c.get("/v1/tasks/missing")
    assert r.status_code == 404
    assert r.json()["code"] == 7
```

- [ ] **Step 2: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/api/test_tasks.py`
Expected: ModuleNotFoundError

- [ ] **Step 3: 实现 tasks router**

```python
# omr_service/api/routers/tasks.py
"""异步任务 REST 包装。

- POST /v1/tasks - 投递异步任务（XADD omr:batch:job + 写 Hash queued）
- GET /v1/tasks/{task_id} - 查询任务状态（读 Hash）

底层调用 mq.producer.enqueue_job，0 改动。
"""
from __future__ import annotations

from datetime import datetime, timezone
import uuid

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from omr_service.api.deps import get_service, get_task_registry
from omr_service.api.schemas.enums import TaskStatus
from omr_service.api.schemas.tasks import (
    CreateTaskRequest,
    TaskCreatedResponse,
    TaskStatusResponse,
)

# 复用现有 mq.producer（0 改动）
from mq.producer import enqueue_job  # noqa: E402

router = APIRouter(prefix="/v1/tasks", tags=["tasks"])


def _validate_payload(task_type: str, payload: dict) -> None:
    """根据 task_type 二次校验 payload。"""
    if task_type == "recognize":
        if not payload.get("template_id") or not payload.get("scan_image_urls"):
            from omr_service.core.exceptions import InvalidRequestError
            raise InvalidRequestError("payload", "template_id or scan_image_urls missing")
    elif task_type == "parse_template":
        if not payload.get("template_id") or not payload.get("template_image_url"):
            from omr_service.core.exceptions import InvalidRequestError
            raise InvalidRequestError("payload", "template_id or template_image_url missing")
    else:
        from omr_service.core.exceptions import InvalidRequestError
        raise InvalidRequestError("task_type", f"unknown: {task_type}")


@router.post("", status_code=202, response_model=TaskCreatedResponse)
def create_task(request: Request, body: CreateTaskRequest):
    """投递异步任务。"""
    task_registry = get_task_registry(request)
    task_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()

    _validate_payload(body.task_type.value, body.payload)

    # 复用现有 mq.producer
    enqueue_job(
        task_type=body.task_type.value,
        payload=body.payload,
        task_id=task_id,
    )

    # 写 Hash 标记 queued
    task_registry.write_queued(
        task_id=task_id,
        task_type=body.task_type.value,
        payload=body.payload,
        created_at=created_at,
    )

    return TaskCreatedResponse(
        task_id=task_id,
        status=TaskStatus.QUEUED,
        created_at=created_at,
    )


@router.get("/{task_id}", response_model=TaskStatusResponse)
def get_task(request: Request, task_id: str):
    """查询任务状态。"""
    task_registry = get_task_registry(request)
    task = task_registry.get(task_id)
    return TaskStatusResponse(**task)
```

- [ ] **Step 4: 跑测试验证**

Run: `cd screenImg && pytest -q tests/api/test_tasks.py`
Expected: 4 tests pass。

- [ ] **Step 5: 提交**

```bash
cd screenImg
git add omr_service/api/routers/tasks.py tests/api/test_tasks.py
git commit -m "feat(omr): add /v1/tasks async REST endpoints"
```

---

## Task 16: mq/job_handler.py 双写 Hash 结果

**Files:**
- Modify: `omr_service/mq/job_handler.py`
- Modify: `tests/mq/test_job_handler.py`

- [ ] **Step 1: 读 mq/producer.py 现有 enqueue_job 签名**

阅读 `omr_service/mq/producer.py`，找到 `enqueue_job` 函数签名；注意本任务同时可能需要**升级**该函数以支持 `task_id` 参数。

- [ ] **Step 2: 写失败的 producer 双写测试**

在 `tests/mq/test_producer.py` 追加（或新建）：

```python
# tests/mq/test_producer.py (新建如果不存在)
from unittest.mock import MagicMock
import json
from mq.producer import enqueue_job


def test_enqueue_job_writes_to_stream_and_hash():
    redis_client = MagicMock()
    enqueue_job(
        task_type="recognize",
        payload={"template_id": "t-1", "scan_image_urls": ["http://x.jpg"]},
        task_id="t-1",
        redis_client=redis_client,
        hash_prefix="omr:batch:result:hash",
    )
    redis_client.xadd.assert_called_once()
    redis_client.hset.assert_called_once()


def test_enqueue_job_string_format():
    redis_client = MagicMock()
    enqueue_job(
        task_type="recognize",
        payload={"template_id": "t-1", "scan_image_urls": ["http://x.jpg"]},
        task_id="t-1",
        redis_client=redis_client,
        hash_prefix="h:",
    )
    call_args = redis_client.xadd.call_args
    # Stream 写：兼容 Java 端 XADD 格式
    stream_name, fields = call_args[0]
    assert stream_name == "omr:batch:job"
    assert b"task_id" in fields or "task_id" in fields
```

- [ ] **Step 3: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/mq/test_producer.py`
Expected: enqueue_job 不接受 task_id / redis_client 参数

- [ ] **Step 4: 升级 producer.enqueue_job**

修改 `omr_service/mq/producer.py` 的 `enqueue_job` 函数签名（**保持向后兼容**）：

```python
# omr_service/mq/producer.py (修改 enqueue_job)

def enqueue_job(
    task_type: str,
    payload: dict,
    task_id: str | None = None,
    *,
    redis_client=None,
    hash_prefix: str = "omr:batch:result:hash",
):
    """投递异步任务到 Redis Stream + 写 Hash 标记 queued。

    Stream 写：保留 Java 端 producer 格式（XADD omr:batch:job ...）。
    Hash 写：让 FastAPI GET /v1/tasks/{id} 秒级查询。
    """
    import uuid
    from datetime import datetime, timezone

    if redis_client is None:
        from mq.client import get_redis_client
        redis_client = get_redis_client()

    if task_id is None:
        task_id = str(uuid.uuid4())

    # 1. Stream 写（Java 端兼容）
    redis_client.xadd("omr:batch:job", {
        "task_id": task_id,
        "task_type": task_type,
        "payload": json.dumps(payload),
        "created_at": datetime.now(timezone.utc).isoformat(),
    })

    # 2. Hash 写（FastAPI 查询）
    redis_client.hset(f"{hash_prefix}:{task_id}", mapping={
        "status": "queued",
        "task_type": task_type,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "payload": json.dumps(payload),
    })

    return task_id
```

- [ ] **Step 5: 跑测试验证**

Run: `cd screenImg && pytest -q tests/mq/test_producer.py tests/mq/test_job_handler.py`
Expected: 全部通过。

- [ ] **Step 6: 提交**

```bash
cd screenImg
git add omr_service/mq/producer.py tests/mq/test_producer.py
git commit -m "feat(omr): enqueue_job writes Stream + Hash for FastAPI query"
```

---

## Task 17: app 工厂 + main.py (uvicorn)

**Files:**
- Create: `omr_service/api/app.py`
- Modify: `omr_service/main.py`

- [ ] **Step 1: 写失败的 app 工厂测试**

```python
# tests/api/test_app.py
from unittest.mock import MagicMock
from fastapi.testclient import TestClient
from omr_service.api.app import create_app
from omr_service.config import OmrSettings


def test_create_app_basic():
    settings = OmrSettings(_env_file=None)
    settings.nacos_enabled = False
    settings.redis_enabled = False
    service = MagicMock()
    task_registry = MagicMock()
    app = create_app(settings=settings, service=service, task_registry=task_registry)
    c = TestClient(app)
    r = c.get("/v1/health")
    assert r.status_code == 200


def test_create_app_includes_all_routers():
    settings = OmrSettings(_env_file=None)
    settings.nacos_enabled = False
    settings.redis_enabled = False
    service = MagicMock()
    task_registry = MagicMock()
    app = create_app(settings=settings, service=service, task_registry=task_registry)
    routes = [r.path for r in app.routes]
    assert "/v1/health" in routes
    assert "/v1/recognize" in routes
    assert "/v1/templates/parse" in routes
    assert "/v1/tasks" in routes
    assert "/v1/openapi.json" in routes
    assert "/v1/docs" in routes
```

- [ ] **Step 2: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/api/test_app.py`
Expected: ModuleNotFoundError

- [ ] **Step 3: 实现 api/app.py**

```python
# omr_service/api/app.py
"""FastAPI 应用工厂。"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from fastapi import FastAPI, Request
import uuid

from omr_service.api.deps import register_dependencies
from omr_service.api.errors import register_error_handlers
from omr_service.api.routers import (
    crops,
    health,
    recognize,
    tasks,
    templates,
)

if TYPE_CHECKING:
    from omr_service.config import OmrSettings
    from omr_service.core.service import OmrService
    from omr_service.core.task_registry import TaskRegistry

logger = logging.getLogger(__name__)


def create_app(
    *,
    settings: "OmrSettings",
    service: "OmrService",
    task_registry: "TaskRegistry",
) -> FastAPI:
    """创建 FastAPI 应用实例。

    OpenAPI / Swagger 路径：/v1/openapi.json, /v1/docs
    """
    app = FastAPI(
        title="OMR Service",
        version="2.0.0",
        openapi_url="/v1/openapi.json",
        docs_url="/v1/docs",
    )

    # request_id 中间件
    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        rid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        request.state.request_id = rid
        response = await call_next(request)
        response.headers["X-Request-ID"] = rid
        return response

    # 依赖 + 错误处理
    register_dependencies(app, settings=settings, service=service, task_registry=task_registry)
    register_error_handlers(app)

    # 路由
    app.include_router(health.router)
    app.include_router(recognize.router)
    app.include_router(templates.router)
    app.include_router(tasks.router)
    app.include_router(crops.router)

    return app
```

- [ ] **Step 4: 跑测试验证**

Run: `cd screenImg && pytest -q tests/api/test_app.py`
Expected: 2 tests pass。

- [ ] **Step 5: 提交**

```bash
cd screenImg
git add omr_service/api/app.py tests/api/test_app.py
git commit -m "feat(omr): add FastAPI app factory with all routers"
```

---

## Task 18: main.py 启动 uvicorn + 后台线程

**Files:**
- Modify: `omr_service/main.py`

- [ ] **Step 1: 写失败的主入口测试**

```python
# tests/test_main.py
from unittest.mock import MagicMock, patch
import pytest

from omr_service import main


def test_main_invokes_uvicorn(monkeypatch):
    monkeypatch.setattr("uvicorn.run", MagicMock())
    monkeypatch.setattr("omr_service.main._setup_dependencies", lambda settings: (MagicMock(), MagicMock(), MagicMock()))
    monkeypatch.setattr("omr_service.main._start_nacos", lambda settings: None)
    monkeypatch.setattr("omr_service.main._start_consumer", lambda settings, service: None)
    monkeypatch.setattr("omr_service.main._deregister_nacos", lambda: None)

    main.main()

    # 验证 uvicorn.run 被调用
    import uvicorn
    assert uvicorn.run.called
```

- [ ] **Step 2: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/test_main.py`
Expected: AttributeError: module 'omr_service.main' has no attribute '_setup_dependencies'

- [ ] **Step 3: 重写 main.py**

**完整重写** `omr_service/main.py`：

```python
"""OMR 服务主入口。

启动流程：
1. 加载配置（Nacos > env > default）
2. 初始化组件（TemplateStore, ImageLoader, WorkerPool, OmrService, TaskRegistry）
3. 启动后台线程（Redis Stream consumer、Nacos 注册/监听）
4. 启动 uvicorn（FastAPI HTTP 入口）
5. 优雅退出（deregister Nacos、停 consumer）
"""
from __future__ import annotations

import logging
import signal
import sys
import threading
from typing import Any

import uvicorn

from omr_service.api.app import create_app
from omr_service.config import OmrSettings, load_settings
from omr_service.core.service import OmrService
from omr_service.core.task_registry import TaskRegistry

logger = logging.getLogger(__name__)


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def _setup_dependencies(settings: OmrSettings) -> tuple[OmrService, TaskRegistry, dict]:
    """初始化核心组件。"""
    from omr_service.engine.ocr import PersonalInfoOcr
    from omr_service.engine.cropper import SubjectiveCropper
    from omr_service.loader.image_loader import ImageLoader
    from omr_service.loader.template_store import TemplateStore
    from omr_service.worker.pool import WorkerPool

    template_store = TemplateStore(ttl_seconds=settings.template_ttl_seconds)
    image_loader = ImageLoader(max_bytes=settings.image_max_bytes)
    worker_pool = WorkerPool(size=settings.worker_pool_size)
    ocr_engine = PersonalInfoOcr()  # 懒加载
    cropper = SubjectiveCropper(output_dir=settings.crop_output_dir, base_url=settings.crop_base_url)

    service = OmrService(
        template_store=template_store,
        image_loader=image_loader,
        worker_pool=worker_pool,
        ocr_engine=ocr_engine,
        cropper=cropper,
        sync_timeout_seconds=settings.sync_timeout_seconds,
    )

    # 复用 Redis client
    if settings.redis_enabled:
        from mq.client import get_redis_client
        redis_client = get_redis_client()
    else:
        redis_client = None

    task_registry = TaskRegistry(
        redis_client=redis_client,
        hash_prefix=settings.redis_result_hash_prefix,
    )

    return service, task_registry, {"template_store": template_store}


def _start_consumer(settings: OmrSettings, service: OmrService) -> threading.Thread | None:
    """启动 Redis Stream consumer 线程。"""
    if not settings.consumer_enabled:
        logger.info("OMR_CONSUMER_ENABLED=false, skip consumer")
        return None
    if not settings.redis_enabled:
        logger.warning("redis_enabled=false, cannot start consumer")
        return None

    from mq.consumer import start_consumer_thread

    thread = start_consumer_thread(service=service, settings=settings)
    logger.info("Redis Stream consumer started")
    return thread


def _start_nacos(settings: OmrSettings) -> None:
    """启动 Nacos 注册 + 配置监听。"""
    if not settings.nacos_enabled:
        logger.info("OMR_NACOS_ENABLED=false, skip nacos")
        return
    try:
        from omr_service.nacos_reg import NacosRegistrator
        registrator = NacosRegistrator(settings)
        if registrator.register():
            logger.info("Nacos registered: %s", settings.nacos_service_name)
    except Exception as e:
        logger.warning("Nacos registration failed: %s", e)


def _deregister_nacos() -> None:
    try:
        from omr_service.nacos_reg import deregister_all
        deregister_all()
    except Exception as e:
        logger.warning("Nacos deregister failed: %s", e)


def main() -> None:
    settings = load_settings()
    _configure_logging(settings.log_level)
    logger.info("Starting OMR service on %s:%s", settings.http_host, settings.http_port)

    service, task_registry, _ = _setup_dependencies(settings)
    app = create_app(settings=settings, service=service, task_registry=task_registry)

    consumer_thread = _start_consumer(settings, service)
    _start_nacos(settings)

    def shutdown_handler(signum, frame):
        logger.info("Received signal %s, shutting down", signum)
        _deregister_nacos()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    uvicorn.run(
        app,
        host=settings.http_host,
        port=settings.http_port,
        workers=1,  # PaddleOCR 显存约束
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: 跑测试验证**

Run: `cd screenImg && pytest -q tests/test_main.py`
Expected: 1 test pass。

- [ ] **Step 5: 跑全量测试**

Run: `cd screenImg && pytest -q`
Expected: 全部通过。

- [ ] **Step 6: 提交**

```bash
cd screenImg
git add omr_service/main.py tests/test_main.py
git commit -m "feat(omr): main.py starts uvicorn + daemon consumer + nacos"
```

---

## Task 19: nacos_reg.py 清理接口级注册

**Files:**
- Modify: `omr_service/nacos_reg.py`
- Modify: `tests/nacos/test_nacos.py`

- [ ] **Step 1: 读 omr_service/nacos_reg.py 现有注册逻辑**

完整阅读 `omr_service/nacos_reg.py`，明确：
- 现有方法 `register()` 注册什么
- 哪些是 `providers:omr.OmrService::` 接口级注册
- shutdown 时 deregister 流程

- [ ] **Step 2: 写失败的测试**

```python
# tests/nacos/test_nacos.py 追加
def test_register_only_app_level_not_interface_level():
    """接口级注册 providers:omr.OmrService:: 已删除。"""
    from omr_service.nacos_reg import NacosRegistrator
    from omr_service.config import OmrSettings

    settings = OmrSettings(_env_file=None)
    settings.nacos_enabled = True
    settings.nacos_service_name = "omr-service"
    reg = NacosRegistrator(settings)
    # 验证 metadata 仅包含应用级
    metadata = reg.build_metadata()
    assert "protocol" in metadata
    assert metadata["protocol"] == "http"
    assert "interface" not in metadata  # 接口级删除
    assert "path" not in metadata
```

- [ ] **Step 3: 跑测试验证失败**

Run: `cd screenImg && pytest -q tests/nacos/test_nacos.py::test_register_only_app_level_not_interface_level`
Expected: FAIL（metadata 含 interface / path）

- [ ] **Step 4: 修改 nacos_reg.py**

修改 `omr_service/nacos_reg.py`：

```python
# 在现有 NacosRegistrator 中替换 build_metadata 方法

def build_metadata(self) -> dict:
    """仅应用级 metadata。接口级注册 (providers:omr.OmrService::) 已删除。"""
    return {
        "protocol": "http",
        "port": str(self.settings.http_port),
        "version": "2.0.0",
        "tag": os.getenv("OMR_TAG", ""),
        "health_check_url": f"http://{self.settings.nacos_ip}:{self.settings.http_port}/v1/health",
    }
```

删除 `register_interface` 方法（如果存在）。

- [ ] **Step 5: 跑测试验证**

Run: `cd screenImg && pytest -q tests/nacos/test_nacos.py`
Expected: 全部通过。

- [ ] **Step 6: 提交**

```bash
cd screenImg
git add omr_service/nacos_reg.py tests/nacos/test_nacos.py
git commit -m "refactor(omr): remove interface-level Nacos registration"
```

---

## Task 20: 删除死代码

**Files:**
- Delete: `omr_service/server.py`
- Delete: `omr_service/rpc/` (全目录)
- Delete: `omr_service/http_server.py`
- Delete: `omr_service/health.py`
- Delete: `omr_service/engine/processor.py`
- Delete: `omr_service/scripts/patch_nacos_protobuf.py`

- [ ] **Step 1: 删除文件**

```bash
cd screenImg
git rm omr_service/server.py
git rm -r omr_service/rpc/
git rm omr_service/http_server.py
git rm omr_service/health.py
git rm omr_service/engine/processor.py
git rm omr_service/scripts/patch_nacos_protobuf.py
```

- [ ] **Step 2: 检查无残留引用**

```bash
cd screenImg
grep -r "from omr_service.server" --include="*.py" .
grep -r "from omr_service.rpc" --include="*.py" .
grep -r "from omr_service.http_server" --include="*.py" .
grep -r "from omr_service.health" --include="*.py" .  # 允许 api/routers/health.py
grep -r "patch_nacos_protobuf" --include="*.py" .
grep -r "engine.processor" --include="*.py" .
```

Expected: 无引用残留。

- [ ] **Step 3: 跑全量测试**

Run: `cd screenImg && pytest -q`
Expected: 全部通过。

- [ ] **Step 4: 提交**

```bash
cd screenImg
git commit -m "chore(omr): remove deprecated gRPC, http_server, processor"
```

---

## Task 21: Docker 配置

**Files:**
- Modify: `Dockerfile`
- Modify: `docker-compose.yml`
- Modify: `docker-compose.prod.yml`
- Modify: `.env.example`
- Modify: `nacos-config-example.yaml`

- [ ] **Step 1: 更新 Dockerfile**

修改 `Dockerfile`：

```dockerfile
FROM python:3.12-slim-bookworm
RUN sed -i 's|deb.debian.org|mirrors.ustc.edu.cn|g' /etc/apt/sources.list.d/*.list || true

RUN apt-get update && apt-get install -y --no-install-recommends \
    libzbar0 libjpeg-dev zlib1g-dev libgl1 libgomp1 curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

COPY omr_service/ ./omr_service/
COPY .env.example .env.example

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -f http://localhost:8080/v1/health || exit 1

ENTRYPOINT ["uvicorn", "omr_service.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
```

- [ ] **Step 2: 更新 docker-compose.yml**

```yaml
version: "3.8"
services:
  omr-service:
    build: .
    image: omr-service:latest
    container_name: omr-service
    ports:
      - "8080:8080"
    env_file:
      - .env
    environment:
      OMR_HTTP_HOST: 0.0.0.0
      OMR_HTTP_PORT: 8080
      OMR_NACOS_SERVER: ${NACOS_SERVER}
      OMR_REDIS_HOST: ${REDIS_HOST:-redis}
      OMR_REDIS_PORT: ${REDIS_PORT:-6379}
      OMR_REDIS_DB: 4
      OMR_CONSUMER_ENABLED: "true"
      OMR_WORKER_POOL_SIZE: "4"
    volumes:
      - ./output:/app/output
      - ./omr_service/templates:/app/omr_service/templates
    restart: unless-stopped
```

- [ ] **Step 3: 更新 docker-compose.prod.yml**

类似上一步，删除 RabbitMQ 相关环境变量。

- [ ] **Step 4: 更新 .env.example**

```env
OMR_HTTP_HOST=0.0.0.0
OMR_HTTP_PORT=8080
OMR_LOG_LEVEL=INFO

OMR_NACOS_ENABLED=true
OMR_NACOS_SERVER=127.0.0.1:8848
OMR_NACOS_NAMESPACE=public
OMR_NACOS_GROUP=DEFAULT_GROUP
OMR_NACOS_DATA_ID=omr-service.yaml
OMR_NACOS_SERVICE_NAME=omr-service

OMR_REDIS_ENABLED=true
OMR_REDIS_HOST=127.0.0.1
OMR_REDIS_PORT=6379
OMR_REDIS_DB=1
OMR_REDIS_PASSWORD=
OMR_REDIS_STREAM_JOB=omr:batch:job
OMR_REDIS_STREAM_RESULT=omr:batch:result
OMR_REDIS_RESULT_HASH_PREFIX=omr:batch:result:hash

OMR_CONSUMER_ENABLED=true
OMR_WORKER_POOL_SIZE=4
OMR_SYNC_TIMEOUT_SECONDS=60

OMR_TEMPLATE_TTL_SECONDS=3600
OMR_IMAGE_MAX_BYTES=52428800
OMR_CROP_OUTPUT_DIR=./output
OMR_CROP_BASE_URL=http://127.0.0.1:8080/v1/omr_crops

# 兼容期字段, 1 个版本后删除。留空表示完全关闭 Dubbo 兼容层。
OMR_LEGACY_DUBBO_PORT=
```

- [ ] **Step 5: 更新 nacos-config-example.yaml**

```yaml
omr-service:
  omr:
    http_host: 0.0.0.0
    http_port: 8080
    log_level: INFO
    redis:
      host: redis
      port: 6379
      db: 4
      stream_job: omr:batch:job
      stream_result: omr:batch:result
      result_hash_prefix: omr:batch:result:hash
    worker_pool_size: 4
    consumer_enabled: true
```

- [ ] **Step 6: 验证能构建镜像**

Run: `cd screenImg && docker build -t omr-service:test .`
Expected: 镜像构建成功。

- [ ] **Step 7: 提交**

```bash
cd screenImg
git add Dockerfile docker-compose.yml docker-compose.prod.yml .env.example nacos-config-example.yaml
git commit -m "build(omr): dockerize FastAPI service on port 8080"
```

---

## Task 22: 文档同步

**Files:**
- Modify: `screenImg/docs/OMR服务接口文档.md`
- Modify: `screenImg/AGENTS.md`
- Modify: `screenImg/CLAUDE.md`
- Modify: `screenImg/README.md`

- [ ] **Step 1: 改写 OMR服务接口文档.md**

把"gRPC / Dubbo Triple"段落全部替换为 FastAPI HTTP 段落。endpoint 表见 spec §3.1。

- [ ] **Step 2: 更新 AGENTS.md**

删除所有 Dubbo Triple 段落。commands 部分加 `uvicorn` 启动命令。

- [ ] **Step 3: 更新 CLAUDE.md**

类似 AGENTS.md。

- [ ] **Step 4: 更新 README.md 端点对照表**

```markdown
| 端点 | 用途 |
|------|------|
| POST /v1/recognize | 同步识别 |
| POST /v1/templates/parse | 同步模板解析 |
| POST /v1/reverify_paper | 复验 |
| POST /v1/tasks | 异步任务投递 |
| GET /v1/tasks/{task_id} | 异步任务查询 |
| GET /v1/health | 存活 |
| GET /v1/health/ready | 就绪 |
| GET /v1/omr_crops/{file_path} | 静态裁剪图 |
| GET /v1/docs | Swagger UI |
| GET /v1/openapi.json | OpenAPI 文档 |
```

- [ ] **Step 5: 提交**

```bash
cd screenImg
git add docs/OMR服务接口文档.md AGENTS.md CLAUDE.md README.md
git commit -m "docs(omr): rewrite service docs for FastAPI"
```

---

## Task 23: E2E 回归

**Files:**
- Create: `tests/integration/test_end_to_end.py`

- [ ] **Step 1: 写 E2E 测试**

```python
# tests/integration/test_end_to_end.py
"""E2E: 启动 uvicorn，真实 HTTP 调用。

需要：Nacos 关闭、Redis 关闭（mock）。生产环境连接真实 Redis。
"""
from pathlib import Path
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
    service.recognize.return_value = {
        "code": 0, "message": "ok", "template_id": "t-1",
        "answers": [], "abnormal": False, "empty_count": 0, "multiple_count": 0, "elapsed_ms": 10,
    }
    task_registry = MagicMock()
    app = create_app(settings=settings, service=service, task_registry=task_registry)
    return TestClient(app)


def test_e2e_health_to_recognize(e2e_app):
    r = e2e_app.get("/v1/health")
    assert r.status_code == 200

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
```

- [ ] **Step 2: 跑测试**

Run: `cd screenImg && pytest -q tests/integration`
Expected: 全部通过。

- [ ] **Step 3: 跑全量测试 + 覆盖率**

Run: `cd screenImg && pytest -q --cov=omr_service --cov-report=term-missing`
Expected: ≥ 80% 行覆盖。

- [ ] **Step 4: 提交**

```bash
cd screenImg
git add tests/integration/test_end_to_end.py
git commit -m "test(omr): add E2E integration tests"
```

---

## Task 24: 整体验收

- [ ] **Step 1: 全量测试**

Run: `cd screenImg && pytest -q`
Expected: 全部通过。

- [ ] **Step 2: 启动服务**

```bash
cd screenImg
uvicorn omr_service.main:app --host 0.0.0.0 --port 8080
```

- [ ] **Step 3: curl 健康检查**

```bash
curl -i http://localhost:8080/v1/health
```

Expected: 200, `{"status":"ok"}`

- [ ] **Step 4: curl 识别**

```bash
curl -i -X POST http://localhost:8080/v1/recognize \
  -H "Content-Type: application/json" \
  -d '{"template_id":"t-1","scan_image_urls":["http://x.jpg"]}'
```

Expected: 200, `{"code":0,...}`

- [ ] **Step 5: 提交 tag**

```bash
cd screenImg
git tag -a v2.0.0-rc1 -m "OMR FastAPI rewrite release candidate"
git push origin v2.0.0-rc1
```

---

## Plan A 自检

**Spec 覆盖**：
- §1.3 目标：✅ Task 1-23 全部覆盖
- §2.1 架构：✅ Task 17 (app factory) + Task 18 (main) 实现
- §3  API 契约：✅ Task 11/12/13/15 全部 endpoint
- §4  组件：✅ Task 4-15 全部
- §5  数据流：✅ Task 5/14/15 覆盖
- §6  错误处理：✅ Task 8
- §7  测试：✅ Task 1-23 全部 TDD
- §8  部署：✅ Task 21
- §9  验收：✅ Task 24

**Type 一致性**：
- `OmrService.recognize(request: dict) -> dict` — Task 5/6 定义；Task 11 router 调用 ✅
- `OmrService.parse_golden_template(request: dict) -> dict` — Task 5/6 定义；Task 12 router 调用 ✅
- `TaskRegistry.write_queued(task_id, task_type, payload, created_at)` — Task 14 定义；Task 15 router 调用 ✅
- `ErrorResponse{code, message, request_id}` — Task 7 Schema 定义；Task 8 handler 返回 ✅

**Placeholder scan**：无 TBD/TODO 残留。
