# OMR FastAPI 改造设计稿

日期: 2026-07-28
范围: `screenImg/` OMR 服务
负责人: 待指定（请在审阅时填写）
状态: 草稿（待用户审阅）

## 1. 背景与目标

### 1.1 现状

`screenImg/` 是一个 Python OMR 答题卡识别微服务，部署在 `exam-ruoyi-cloud` 项目内。当前形态（调研于 2026-07-28）：

- 主入口 `omr_service/main.py`，通过 `python -m omr_service.main` 启动
- 同时暴露三种入口：
  - **gRPC / Dubbo Triple** `:20884`（`omr_service/server.py` + `omr_service/rpc/omr_service.py`）—— 生产实际未启用
  - **HTTP fallback** `:20885`（`omr_service/http_server.py`，基于 stdlib `http.server`）—— **生产实际调用走这里**
  - **健康检查** HTTP（`omr_service/health.py`，`OmrConfig.health_port` 默认 `9173`，但 Dockerfile / Compose 暴露 `8080`，**端口与人错位**）
- 内部异步通道：**Redis Stream** `omr:batch:job` → `omr:batch:result`（`omr_service/mq/`）—— 由 Java `ExamPaperTemplateOmrJobServiceImpl` / `ExamOmrBatchServiceImpl` 投递
- 服务发现与配置：**Nacos**（`omr_service/nacos_config.py` + `nacos_reg.py`），同时承担配置中心和应用级 / 接口级注册
- Java 端 `exam-admin` 默认 `omr.use-dubbo=false`（`application.yml:8`），通过 JDK `HttpClient` POST JSON 调用 `:20885` 同步路径
- `/recognize_by_template` / `/parse_golden_template` 走 HTTP 同步；`/verify_recognition_rate` 在 HTTP fallback 中返回 501；`/reverify_paper` 与 `/recognize_by_template` 共用底层实现

### 1.2 痛点

1. **HTTP fallback 代码质量差**：`http_server.py` 500+ 行手写 JSON ↔ protobuf 转换，无 schema 校验，无 OpenAPI，无 middleware
2. **Dubbo Triple 是死代码**：生产从未启用，但承担 200+ 行 stub + protobuf 编译产物 + Triple 端口 + 接口级 Nacos 注册
3. **入口分裂**：gRPC + HTTP fallback + health 三个 HTTP/网络入口并存，运维心智负担重
4. **错误处理分散**：异常处理在 `http_server.py`、`rpc/omr_service.py`、`mq/job_handler.py` 三处重复
5. **健康端口错位**：代码 9173 / Docker 8080 / 文档 9173 / Compose 8080，四处不一致
6. **缺少面向前端的 REST 入口**：H5 / Vben5 想要"上传图片 → 拿结果"目前只能绕到 exam-admin，调 OMR 私有协议不可见

### 1.3 目标

- **用 FastAPI 替换 `http_server.py`**，得到 OpenAPI、Pydantic 校验、统一 middleware
- **干掉 Dubbo Triple `:20884` 死代码**，端口、`server.py` 删除；`nacos_reg.py` 修改：去掉接口级注册，保留应用级注册
- **统一健康端口为 8080**，修复 Docker 镜像与代码不一致
- **新增 REST 任务入口** `POST /v1/tasks` + `GET /v1/tasks/{id}`，把 Redis Stream 包装成 REST（当前由 exam-admin 调用；是否对 H5/Vben5 开放见 §12 #3）
- **Java 端改造范围**：异步路径（Redis Stream）继续工作，**0 改动**；同步路径需要适配新 URL / JSON 字段（详见 §3.8 与 §8.5-8.8）。

### 1.4 非目标

- 不改 `engine/` 与 `loader/` 内部识别算法
- 不改 `mq/` 内部 Redis Stream 实现
- 不切换 Java 端 `exam-admin` 的调用方式（同步仍 HTTP、异步仍 Redis Stream）
- 不引入异步 OMR 识别（CPU/GIL 重工作仍走同步函数进入 anyio 线程池）
- 不做微服务拆分（保持单进程单镜像）

## 2. 架构

### 2.1 整体架构图

```
┌──────────────────────────────────────────────────────────────────────┐
│                       exam-admin (Java, Spring Boot)                 │
│      ExamPaperTemplateServiceImpl ──POST /v1/recognize──┐            │
│      ExamOmrBatchServiceImpl ────XADD omr:batch:job ────┤            │
│                                                       │            │
└───────────────────────────────────────────────────────┼────────────┘
                                                        │ HTTP/JSON
                                                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     uvicorn (omr_service.main:app)                   │
│                       FastAPI 0.115+ (Python 3.11+)                  │
│                                                                      │
│  ┌────────────── api/ ──────────────┐  ┌────────── async ──────────┐  │
│  │ routers/                       │  │ WorkerPool (异步批处理)   │  │
│  │  ├── recognize.py   sync        │  │  ←── Redis Stream consumer│  │
│  │  ├── templates.py   sync        │  │       (omr:batch:job)    │  │
│  │  ├── tasks.py       async REST  │  │                          │  │
│  │  ├── health.py                  │  │                          │  │
│  │  └── crops.py                   │  │                          │  │
│  └────────────────┬────────────────┘  └────────────┬─────────────┘  │
│                   │                                │                │
│                   ▼                                ▼                │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  core/service.py  (protocol-agnostic 业务编排)                  │   │
│  │   ├── Same OmrServiceServicer core logic                       │   │
│  │   ├── Receives plain Python dicts (no protobuf)                │   │
│  │   └── Returns plain Python dicts                               │   │
│  └─────────┬──────────────────────────────────────┬───────────────┘   │
│            │                                      │                   │
│            ▼                                      ▼                   │
│  ┌──────────────┐                       ┌──────────────────────────┐  │
│  │ engine/      │                       │ loader/                  │  │
│  │ standard_    │                       │  image_loader.py         │  │
│  │ template.py  │                       │  template_store.py       │  │
│  │ recognizer.py│                       │                          │  │
│  │ ocr.py       │                       │                          │  │
│  │ cropper.py   │                       │                          │  │
│  └──────────────┘                       └──────────────────────────┘  │
│                                                                      │
│  config (Pydantic Settings)         nacos_config / nacos_reg         │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### 2.2 关键不变量

- **API 边界即 Pydantic 边界**。`core/service.py` 与 `engine/` 完全不感知 Pydantic、protobuf、FastAPI。所有跨层数据用 plain dict + 简单 dataclass（保持 `engine/` 0 改动）。
- **`engine/` 与 `loader/` 0 改动**。核心识别算法完全保留。
- **`mq/` 内部 0 改动**。Redis Stream producer/consumer 仍然存在，Java 端现有异步任务链路完全不打搅。
- **`config.py` 重构**：`dataclass` → `Pydantic Settings`；`dubbo_port` 改名 `http_port`；`OMR_DUBBO_PORT` → `OMR_HTTP_PORT`（保留别名过渡 1 个版本）。
- **`main.py` 简化**：单一 `uvicorn.run(...)` 启动；`OMR_CONSUMER_ENABLED=false` 时**只关闭 consumer 线程**，producer（`POST /v1/tasks` 写入 stream）继续工作（生产端必须始终可用，否则 REST 投递会失败）。
- **路由函数全用 `def`（同步）**。并发受 `WorkerPool` 限制；anyio 线程池默认 40 thread 足够，PaddleOCR 内存峰值需监控。
- **WorkerPool vs anyio 线程池是两个不同机制**：
  - **anyio 线程池**：FastAPI 把 `def` 路由函数 offload 到的默认线程池，默认 40 thread。每个 HTTP 请求占用 1 个线程执行同步函数体；函数体内部调用 `OmrService.recognize()` 会进一步受 WorkerPool 限流。
  - **WorkerPool**：业务级信号量（默认 4），控制同时进入 PaddleOCR / OpenCV 的请求数。即使 anyio 线程池有 40 个空位，OMR 实际并发也 ≤ 4。
- **uvicorn 启动 `--workers 1`**。PaddleOCR 是类级懒加载单例，多 worker 会复制多份显存。

### 2.3 进程模型

```
uvicorn (--workers 1)
  ├── asyncio event loop (uvicorn 主协程)
  │     ├── FastAPI routers
  │     └── health checks
  │
  ├── anyio thread pool (默认 40 thread, 跑 sync 路由函数)
  │
  ├── Redis Stream consumer thread (daemon, 启停由 OMR_CONSUMER_ENABLED 控制)
  │     └── WorkerPool (限并发, 默认 4)
  │
  └── Nacos config listener thread (daemon)
```

进程内不跨 worker 通信，所有状态（`TemplateStore`、PaddleOCR 模型）都是单实例。

## 3. API 契约

### 3.1 端点列表

| Method | Path | 用途 | 作用 |
|---|---|---|---|
| POST | `/v1/recognize` | 同步识别 | 替代原 HTTP `/recognize_by_template` |
| POST | `/v1/templates/parse` | 同步模板解析 | 替代原 HTTP `/parse_golden_template` |
| POST | `/v1/verify_recognition_rate` | 识别率验证 | 暂返回 501（保留路由，行为不变） |
| POST | `/v1/reverify_paper` | 复验 | 与 `/v1/recognize` 行为等价（共用 schema） |
| POST | `/v1/tasks` | 异步任务投递 | 包装 `omr:batch:job` producer |
| GET | `/v1/tasks/{task_id}` | 异步任务查询 | 包装 `omr:batch:result` 读取 |
| GET | `/v1/health` | 存活探针 | 统一 8080 |
| GET | `/v1/health/ready` | 就绪探针 | 检查 TemplateStore、Redis、Nacos |
| GET | `/v1/omr_crops/{file_path:path}` | 静态裁剪图 | 替代原 `/omr_crops/...`（file_path 含子路径） |
| GET | `/v1/openapi.json` | OpenAPI 文档 | FastAPI 自动生成（`FastAPI(openapi_url="/v1/openapi.json")`） |
| GET | `/v1/docs` | Swagger UI | FastAPI 自动生成（`FastAPI(docs_url="/v1/docs")`） |

### 3.2 同步请求：`POST /v1/recognize`

**Request**:
```json
{
  "template_id": "exam-2026-q1",
  "scan_image_urls": ["https://oss.example.com/scan/page1.jpg", "https://oss.example.com/scan/page2.jpg"],
  "question_no": 1
}
```

**Response 200**:
```json
{
  "code": 0,
  "message": "ok",
  "elapsed_ms": 8234,
  "template_id": "exam-2026-q1",
  "answers": [
    {
      "question_no": 1,
      "answer_type": "single",
      "selected": ["A"],
      "is_blank": false,
      "is_multiple": false,
      "confidence": 0.95
    }
  ],
  "empty_count": 2,
  "multiple_count": 0,
  "abnormal": false,
  "personal_info": {
    "name": "张三",
    "exam_id": "B20260101",
    "raw_text": "..."
  },
  "subjective_crops": [
    {
      "region_id": "q-essay-1",
      "url": "https://omr.example.com/v1/omr_crops/abc.jpg",
      "width": 1200,
      "height": 800
    }
  ]
}
```

**错误码**（与 `omr.proto` 错误码语义一致）：

| HTTP | code | 含义 |
|---|---|---|
| 200 | 0 | 成功 |
| 404 | 4 | 模板未找到 |
| 502 | 5 | 图片加载失败 |
| 400 | 6 | 请求参数非法 |
| 500 | 99 | 内部错误 |

**字段约定**：`answer_type` 枚举值：`single` / `multiple` / `blank` / `unknown`。

### 3.3 同步请求：`POST /v1/templates/parse`

**Request**:
```json
{
  "template_id": "exam-2026-q1",
  "template_image_url": "https://oss.example.com/template.jpg",
  "columns": [
    {
      "column_id": "col-1",
      "column_index": 0,
      "question_start": 1,
      "question_count": 20,
      "options_per_question": 4,
      "question_type": "single"
    }
  ],
  "personal_info_region": {
    "x": 100, "y": 50, "width": 800, "height": 200
  },
  "subjective_regions": [
    {
      "region_id": "q-essay-1",
      "x": 100, "y": 1000, "width": 1000, "height": 600
    }
  ]
}
```

**Response 200**:
```json
{
  "code": 0,
  "message": "ok",
  "template_id": "exam-2026-q1",
  "answers": [
    {
      "question_no": 1,
      "answer_type": "single",
      "selected": ["A"],
      "is_blank": false,
      "is_multiple": false
    }
  ],
  "bubble_grid": [
    {"row": 0, "col": 0, "question_no": 1, "option": "A", "x": 120, "y": 240}
  ],
  "personal_info_sample": { "name": "...", "exam_id": "..." },
  "subjective_crops": [...]
}
```

错误码与 `/v1/recognize` 一致。

### 3.4 异步任务：`POST /v1/tasks`

统一接受"识别 / 模板解析"两类任务，**复用 Redis Stream 现有 `task_id` 格式**。生产端 `mq/producer.py` 已使用 `uuid4`（见 §12 开放问题 #2）。FastAPI 侧若需生成 task_id，沿用 `uuid4()`。

**Request**:
```json
{
  "task_type": "recognize",  // "recognize" | "parse_template"
  "payload": {
    "template_id": "exam-2026-q1",
    "scan_image_urls": ["..."]
  }
}
```

**Response 202**:
```json
{
  "task_id": "uuid-xxx",
  "status": "queued",
  "created_at": "2026-07-28T10:00:00Z"
}
```

**payload schema**（用 Pydantic discriminated union）：
- `task_type="recognize"` 时 payload 必填字段：`template_id`（str）、`scan_image_urls`（list[str] ≥ 1）
- `task_type="parse_template"` 时 payload 必填字段：`template_id`、`template_image_url`、`columns`、`personal_info_region`

### 3.5 异步任务：`GET /v1/tasks/{task_id}`

**Response 200 (succeeded)**:
```json
{
  "task_id": "uuid-xxx",
  "status": "succeeded",
  "task_type": "recognize",
  "created_at": "2026-07-28T10:00:00Z",
  "finished_at": "2026-07-28T10:00:08Z",
  "result": { ...完整识别结果... }
}
```

**Response 200 (processing)**:
```json
{
  "task_id": "uuid-xxx",
  "status": "processing",
  "task_type": "recognize",
  "created_at": "2026-07-28T10:00:00Z"
}
```

**Response 200 (failed)**:
```json
{
  "task_id": "uuid-xxx",
  "status": "failed",
  "task_type": "recognize",
  "created_at": "2026-07-28T10:00:00Z",
  "finished_at": "2026-07-28T10:00:05Z",
  "error": { "code": 99, "message": "识别器异常" }
}
```

**Response 404**:
```json
{
  "code": 7,
  "message": "task_id 不存在"
}
```

### 3.6 错误响应（统一格式）

```json
{
  "code": 99,
  "message": "模板未找到",
  "request_id": "req-uuid"
}
```

`request_id` 由 FastAPI 中间件生成（基于 `X-Request-ID` header 或 `uuid4`），写入所有响应与日志。

### 3.7 字段命名规范

- 全字段 `snake_case`
- 枚举值 lowercase
- 时间字段 ISO 8601（`2026-07-28T10:00:00Z`）
- URL 路径全 lowercase，复数名词（`/v1/tasks`、`/v1/templates/parse`）

### 3.8 Java 端契约影响

- **同步路径（`ExamPaperTemplateServiceImpl`）**：HTTP method / path / header / body 字段名都改。Java 侧需要适配：
  - `callOmrRecognizeByTemplateViaHttp` / `callOmrParseGoldenTemplateViaHttp` 的 URL 路径改 `/v1/recognize` / `/v1/templates/parse`
  - JSON 字段名 `scanImageUrl` → `scan_image_urls`（单图转单元素数组：`List.of(url)`，含 `question_no` 字段保留）
  - `QuestionAnswer` 字段 `selected` 保持数组（语义不变）
- **异步路径（`ExamOmrBatchServiceImpl` / `ExamPaperTemplateOmrJobServiceImpl`）**：**0 改动**。继续 `XADD omr:batch:job` 即可。

## 4. 组件设计

### 4.1 目录结构

```
screenImg/
├── omr_service/
│   ├── api/                    # 【新】FastAPI 路由层
│   │   ├── __init__.py
│   │   ├── deps.py             # 依赖注入（service, settings, store, pool）
│   │   ├── errors.py           # 统一异常 → HTTP 响应
│   │   ├── schemas/            # Pydantic models
│   │   │   ├── recognize.py
│   │   │   ├── templates.py
│   │   │   ├── tasks.py
│   │   │   ├── common.py
│   │   │   └── enums.py
│   │   └── routers/
│   │       ├── recognize.py    # POST /v1/recognize
│   │       ├── templates.py    # POST /v1/templates/parse
│   │       ├── tasks.py        # POST /v1/tasks, GET /v1/tasks/{id}
│   │       ├── health.py       # GET /v1/health, /v1/health/ready
│   │       └── crops.py        # GET /v1/omr_crops/{file_path:path}
│   ├── core/                   # 【新】protocol-agnostic 业务层
│   │   ├── __init__.py
│   │   ├── service.py          # OmrService 类（接收 dict, 返回 dict）
│   │   ├── mapper.py           # 兼容 mq/job_handler.py 现有 protobuf 内部 schema；REST 路径不再使用
│   │   └── exceptions.py       # OmrError, TemplateNotFound, ImageLoadError, ...
│   ├── engine/                 # 0 改动
│   │   ├── recognizer.py
│   │   ├── recognizers/standard.py
│   │   ├── standard_template.py
│   │   ├── score_calculator.py
│   │   ├── cropper.py
│   │   ├── ocr.py
│   │   └── personal_info_block_parser.py
│   ├── loader/                 # 0 改动
│   │   ├── image_loader.py
│   │   └── template_store.py
│   ├── mq/                     # 0 改动
│   │   ├── client.py
│   │   ├── consumer.py
│   │   ├── producer.py
│   │   └── job_handler.py
│   ├── nacos_config.py         # 0 改动（保留）
│   ├── nacos_reg.py            # 修改：去掉接口级注册
│   ├── config.py               # 重构：Pydantic Settings
│   ├── main.py                 # 重构：uvicorn.run 启动
│   └── worker/pool.py          # 0 改动
│
├── tests/                       # 【新结构】
│   ├── api/                    # 【新】FastAPI 路由测试
│   │   ├── test_recognize.py
│   │   ├── test_templates.py
│   │   ├── test_tasks.py
│   │   └── test_health.py
│   ├── core/                   # 【新】业务层单元测试
│   │   └── test_service.py
│   ├── engine/                 # 0 改动
│   ├── mq/                     # 0 改动
│   └── conftest.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── docker-compose.prod.yml
├── .env.example
├── nacos-config-example.yaml
├── README.md
└── AGENTS.md
```

### 4.2 删除清单

- `omr_service/server.py`（gRPC server）
- `omr_service/rpc/`（整个目录：`omr.proto`、`omr_pb2.py`、`omr_pb2_grpc.py`、`omr_service.py`、`tag_aware_client.py`）
- `omr_service/http_server.py`（HTTP fallback）
- `omr_service/health.py`（合并到 `api/routers/health.py`）
- `omr_service/engine/processor.py`（死代码：早期模板差分法，**未在 RPC 路径使用**）
- `omr_service/scripts/patch_nacos_protobuf.py`（不再需要 PaddlePaddle 降级 protobuf 到 3.20）
- `omr_service/tests/test_rpc.py`（gRPC 路径已删除，测试文件随之删除；详见 §7.3）

### 4.3 `core/service.py` 接口

```python
from typing import Any

class OmrService:
    def __init__(self, template_store, image_loader, worker_pool, ocr_engine, cropper):
        ...

    def parse_golden_template(self, request: dict[str, Any]) -> dict[str, Any]:
        """接受 dict, 返回 dict. 包含原 OmrServiceServicer._parse_template 逻辑."""

    def recognize(self, request: dict[str, Any]) -> dict[str, Any]:
        """接受 dict, 返回 dict. 包含原 OmrServiceServicer._recognize 逻辑."""

    # verify_recognition_rate 与 reverify_paper 暂保留为 stub, 返回 501
    def verify_recognition_rate(self, request: dict) -> dict:
        raise NotImplementedError("verify_recognition_rate 暂未通过 HTTP 暴露")

    # 请求/响应 schema 与 recognize 完全一致 (复用 RecognizeRequest / RecognizeResponse)
    def reverify_paper(self, request: dict) -> dict:
        return self.recognize(request)
```

**重要约束**：`core/service.py` **不依赖** FastAPI、Pydantic、protobuf。`engine/` 重构后只接受/返回 plain Python 类型。

### 4.4 `config.py` Pydantic Settings

```python
from pydantic_settings import BaseSettings, SettingsConfigDict

class OmrSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="OMR_", case_sensitive=False)

    # 服务
    http_host: str = "0.0.0.0"
    http_port: int = 8080
    # health_port 已删除（合并到 http_port=8080）。保留读取旧 `OMR_HEALTH_PORT` 仅用于 1 版本兼容期 warning 日志。
    log_level: str = "INFO"

    # Nacos (服务发现 + 配置)
    nacos_enabled: bool = True
    nacos_server: str = "127.0.0.1:8848"
    nacos_namespace: str = "public"
    nacos_group: str = "DEFAULT_GROUP"
    nacos_data_id: str = "omr-service.yaml"
    nacos_service_name: str = "omr-service"
    nacos_ip: str = ""  # 自动探测

    # Redis
    redis_enabled: bool = True
    redis_host: str = "127.0.0.1"
    redis_port: int = 6379
    redis_db: int = 1
    redis_password: str = ""
    redis_stream_job: str = "omr:batch:job"
    redis_stream_result: str = "omr:batch:result"
    redis_result_hash_prefix: str = "omr:batch:result:hash"

    # 任务
    consumer_enabled: bool = True
    worker_pool_size: int = 4
    sync_timeout_seconds: float = 60.0

    # OMR 内部
    template_ttl_seconds: int = 3600
    image_max_bytes: int = 50 * 1024 * 1024
    crop_output_dir: str = "./output"
    crop_base_url: str = "http://127.0.0.1:8080/v1/omr_crops"

    # 兼容期
    legacy_dubbo_port: int | None = None  # 保留 1 版本, OMR_DUBBO_PORT 兼容
```

**加载优先级**：`Nacos` > `环境变量/.env` > `Pydantic 默认`（与现状一致）。

### 4.5 `nacos_reg.py` 改动

- 移除 `providers:omr.OmrService::` 接口级注册
- 保留 `omr-service` 应用级注册
- metadata 简化，只保留 `protocol=http`、`port=8080`、`tag=...`、`version=...`
- shutdown 时主动 `deregister()`（不管 SIGTERM / SIGINT / unhandled exception）

### 4.6 `main.py` 启动

```python
def main():
    settings = OmrSettings()
    configure_logging(settings.log_level)

    # 1. 初始化组件
    template_store = TemplateStore(ttl=settings.template_ttl_seconds)
    image_loader = ImageLoader(max_bytes=settings.image_max_bytes)
    worker_pool = WorkerPool(size=settings.worker_pool_size)
    ocr_engine = PersonalInfoOcr()  # 懒加载
    cropper = SubjectiveCropper(...)

    # 2. 业务服务
    service = OmrService(template_store, image_loader, worker_pool, ocr_engine, cropper)

    # 3. FastAPI app
    app = create_app(settings, service)

    # 4. 后台线程（可选）
    if settings.consumer_enabled:
        consumer_thread = start_redis_consumer(settings, service)
    if settings.nacos_enabled:
        start_nacos_listener(settings)
        register_nacos(settings)

    # 5. 启动
    try:
        uvicorn.run(app, host=settings.http_host, port=settings.http_port, workers=1)
    finally:
        deregister_nacos(settings)
        consumer_thread.stop()
```

## 5. 数据流

### 5.1 同步识别流程

```
Client
  │ POST /v1/recognize  {template_id, scan_image_urls, question_no}
  ▼
FastAPI middleware (auth, request_id, logging)
  ▼
Pydantic validation (RecognizeRequest schema)
  ▼
api/routers/recognize.py::recognize() (sync function → anyio thread pool)
  │ dict = request.model_dump()
  ▼
core/service.py::OmrService.recognize(dict)
  │ ImageLoader.load(urls) → list[np.ndarray]
  │ TemplateStore.get(template_id) → template
  │ StandardTemplateRecognizer.recognize(template, images) → raw_result
  │ (optional) PersonalInfoOcr.recognize(images) → personal_info
  │ (optional) SubjectiveCropper.crop(images) → crops
  │ dict = score_calculator.format(raw_result)
  ▼
api/routers/recognize.py::recognize()
  │ return RecognizeResponse(code=0, ...)
  ▼
JSON response
```

### 5.2 异步任务流程

```
Client
  │ POST /v1/tasks  {task_type, payload}
  ▼
api/routers/tasks.py::create_task() (sync)
  │ task_id = uuid4()
  │ from mq.producer import enqueue_job  # 复用现有 producer, 0 改动
  │ enqueue_job(task_type, payload, task_id=task_id)
  │ return TaskCreated(task_id, status="queued", created_at)
  ▼
[异步]
  │
  ▼
Redis Stream consumer thread (daemon)
  │ XREADGROUP omr:batch:job
  │ task_id → match JobHandler
  ▼
omr_service/mq/job_handler.py::handle_task (现有逻辑)
  │ core/service.py::OmrService.recognize(payload) → dict
  │ redis_client.xadd(omr:batch:result, {task_id, status, result, finished_at})
  │ XACK
  │
  ▼
Client
  │ GET /v1/tasks/{task_id}
  ▼
api/routers/tasks.py::get_task() (sync)
  │ HGETALL omr:batch:result:{task_id}  (用 cache hash 而非 stream pop)
  │ return TaskStatus(...)
  ▼
JSON response
```

**关键决策**：异步任务结果**双写**：
- 写 `omr:batch:result` stream（保留 Java 端现状）
- 写 `omr:batch:result:hash:{task_id}` Hash（让 FastAPI 查询）

**强制要求**：写 Hash (`HSET omr:batch:result:hash:{task_id} result <json> ttl <epoch>`) 同时保留 Stream 写。Stream 写保留是为了 §3.8 声明的 Java 端 0 改动。

### 5.3 错误流

```
OmrService raises OmrError(code=4, message="模板未找到")
  ▼
api/routers/recognize.py: 不捕获, 直接 raise OmrError
  ▼
@app.exception_handler(OmrError) (in api/errors.py) 统一映射 HTTP code
  ▼
Client 收到 JSON: {"code": 4, "message": "模板未找到", "request_id": "..."}
```

全局异常 handler 统一捕获 `OmrError`、`ValueError`、`Exception`：

```python
@app.exception_handler(OmrError)
async def omr_error_handler(request, exc: OmrError):
    status_code = {4: 404, 5: 502, 6: 400, 99: 500}.get(exc.code, 500)
    return JSONResponse(status_code=status_code, content={
        "code": exc.code, "message": exc.message, "request_id": request.state.request_id
    })

@app.exception_handler(Exception)
async def unhandled_handler(request, exc):
    logger.exception("unhandled", exc_info=exc)
    return JSONResponse(status_code=500, content={
        "code": 99, "message": "内部错误", "request_id": request.state.request_id
    })
```

## 6. 错误处理

### 6.1 错误码

| code | HTTP | 含义 | 触发场景 |
|---:|---:|---|---|
| 0 | 200 | 成功 | — |
| 4 | 404 | 模板未找到 | TemplateStore.get miss |
| 5 | 502 | 图片加载失败 | HTTP 404/5xx、超时、字节超限 |
| 6 | 400 | 请求参数非法 | Pydantic 校验失败、必填字段缺失 |
| 7 | 404 | 任务不存在 | `GET /v1/tasks/{id}` 未找到 |
| 99 | 500 | 内部错误 | OpenCV 异常、PaddleOCR 异常、未捕获异常 |

### 6.2 错误响应 schema

```json
{
  "code": 99,
  "message": "PaddleOCR 初始化失败",
  "request_id": "req-uuid-xxx"
}
```

### 6.3 重试与超时

- **客户端同步请求**：单次同步调用，不在 FastAPI 层做自动重试（避免重复识别开销）；客户端自行决定。
- **同步超时语义**：`sync_timeout_seconds=60` 是单个 `OmrService.recognize()` 调用的总超时（从 WorkerPool acquire slot 开始算）。超时返回 `504 Gateway Timeout`，HTTP code 504，业务 code 99。**与 Java 端 JDK `HttpClient 3 次重试 + 1s 退避` 正交**：FastAPI 层不重试，超时由 Java client 决定是否重试。
- **异步任务重试**：保留 `mq/job_handler.py` 现有重试逻辑（最多 N 次，写失败结果）。

### 6.4 超时与背压

- 同步识别 WorkerPool 等待时间 = `sync_timeout_seconds`
- WorkerPool 满（`worker_pool_size` 默认 4）时，超出请求直接 `503 Service Unavailable`，`code=99`。anyio 线程池（默认 40）饱和时由 Starlette 默认行为处理（请求在 event loop 排队，不超时）。
- Redis Stream 消费侧 inflight 背压保留（`mq/consumer.py` 现有逻辑）

## 7. 测试策略

### 7.1 单元测试

- `tests/core/test_service.py`：`OmrService` 业务方法，mock engine 层
- `tests/api/test_recognize.py`、`test_templates.py`、`test_tasks.py`、`test_health.py`：FastAPI `TestClient`，覆盖 200/4xx/5xx 三档
- `tests/api/test_schemas.py`：Pydantic schema 校验（必填、类型、enum）

### 7.2 集成测试

- `tests/integration/test_end_to_end.py`：
  - 启动 uvicorn（`lifespan` 模式）
  - `httpx.AsyncClient` 真实 HTTP 调用
  - 使用真实 `testPaper/` 样例图片
  - 异步任务 → 等待 30s → 轮询结果
- `tests/integration/test_redis_stream.py`：保留现有 `test_mq.py` / `test_consumer.py`
- `tests/integration/test_nacos.py`：保留现有 `test_nacos.py`

### 7.3 既有测试

- `omr_service/tests/`（现有）全部迁移到 `tests/`，保持覆盖率不下降
- `test_rpc.py` 在 4.2 节删除清单后**整文件删除**
- `test_integration.py` 中跟 gRPC / HTTP fallback 相关的 case 改写为 FastAPI TestClient

### 7.4 回归测试

- 部署前必须跑：
  - `python -m omr_service.main` 启动一次
  - `curl -X POST http://127.0.0.1:8080/v1/recognize -d '{"template_id":"...","scan_image_urls":["..."]}'`
  - 验证返回 `code:0` 且含 `answers`
- Java 端 `exam-admin` 启动一次，`ExamPaperTemplateServiceImpl.recognizeByTemplate` 跑通
- Redis Stream 投递一次（`XADD omr:batch:job`），等 10s 查 `omr:batch:result` 有结果

### 7.5 覆盖率

- 目标：`tests/core/` 和 `tests/api/` 行覆盖 ≥ 80%
- `engine/` / `loader/` 保留现有覆盖率

### 7.6 测试 runner

- `pytest` + `pytest-asyncio` + `httpx`（替代 unittest discover）
- 命令：`pytest -q --cov=omr_service --cov-report=term-missing`
- 保留 `unittest` 兼容命令 `python -m unittest discover -s tests -p "test_*.py" -v`（仅作 1 个版本过渡）

## 8. 部署与运维

### 8.1 Dockerfile

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

### 8.2 docker-compose.yml

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

### 8.3 .env.example

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

### 8.4 nacos-config-example.yaml

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

### 8.5 Java 端 application.yml 调整

```yaml
# exam-admin/src/main/resources/application.yml
omr:
  http-base-url: ${OMR_HTTP_BASE_URL:http://omr-service:8080}
  sync-timeout-seconds: 60
```

删除（已在 Dubbo 删除后）：
```yaml
# 整段删除
omr:
  use-dubbo: false   # ❌ 删除
  http-base-url: ... # 重命名为上面的形式
```

### 8.6 Java 端 `ExamPaperTemplateServiceImpl` 改动

- 删除 `import org.apache.dubbo.config.annotation.DubboReference`
- 删除 `@DubboReference` 字段
- 删除 `omrUseDubbo` 三元分支相关代码（约 30 行）
- `callOmrRecognizeByTemplateViaHttp` / `callOmrParseGoldenTemplateViaHttp` 方法：
  - URL path 改 `/v1/recognize` / `/v1/templates/parse`
  - JSON 字段名 `scanImageUrl` → `scan_image_urls`
  - 请求体字段映射详见 §3.2 和 §3.3

### 8.7 Java 端 `pom.xml` 调整

- `ruoyi-api/ruoyi-api-exam-admin/pom.xml`：
  - 删除 `org.apache.dubbo:dubbo:3.3.6` 依赖
  - 删除 `protobuf-version` / `grpc-version` 属性
  - 删除 `protobuf-java[-util]`、`grpc-protobuf`、`grpc-stub`、`javax.annotation:javax.annotation-api`
  - 删除 `dubbo-maven-plugin` 插件配置
  - **保留** `ruoyi-common-dubbo` 依赖（其他 exam Dubbo 服务仍用）

### 8.8 Java 端 `ExamPaperTemplateServiceImplTest` 改动

- 删除 `@Mock private OmrService omrService;`
- 删除 `ReflectionTestUtils.setField` 调用
- 用 `MockWebServer`（OkHttp）或 `WireMock` mock OMR HTTP 端点
- 现有 `shouldRecognizeAll*` / `shouldReturnScanResultSuccessfully` 改写为"mock HTTP 返回 RecognizeResult，验证 ServiceImpl 正确解析"

### 8.9 端口变化

- ❌ 删除 `20884` (Dubbo Triple)
- ❌ 删除 `20885` (HTTP fallback)
- ➕ 新增 `8080` (FastAPI + HTTP fallback + health 合并)

## 9. 验收标准

### 9.1 功能验收

- [ ] `POST /v1/recognize` 在 5-30s 内返回 200 + `code:0` + 完整 `answers`
- [ ] `POST /v1/templates/parse` 同步返回正确 GoldenTemplateResult
- [ ] `POST /v1/tasks` 返回 202 + `task_id`，异步消费完成后 `GET /v1/tasks/{id}` 返回 `succeeded`
- [ ] 错误响应统一 schema：`code` / `message` / `request_id`
- [ ] Pydantic schema 校验失败返回 `code:6` + 400
- [ ] 模板未找到返回 `code:4` + 404
- [ ] 图片加载失败返回 `code:5` + 502
- [ ] 健康检查 `/v1/health` 在 8080 端口返回 200
- [ ] `/v1/omr_crops/test.jpg` 静态服务正常

### 9.2 业务验收

- [ ] Java `ExamPaperTemplateServiceImpl.recognizeByTemplate` 通过新 HTTP 路径调用 OMR，返回结果与改造前完全等价
- [ ] Java `ExamOmrBatchServiceImpl` / `ExamPaperTemplateOmrJobServiceImpl` 通过 Redis Stream 投递任务，`omr:batch:result` 收到结果
- [ ] Java `ExamOmrResultConsumer` 正常消费结果
- [ ] H5 / Vben5 不直接调用 OMR（明确不在本 spec 范围，见 §12 #3）

### 9.3 质量验收

- [ ] `tests/core/` 行覆盖 ≥ 80%
- [ ] `tests/api/` 路由覆盖 ≥ 90%
- [ ] `pytest -q` 全绿
- [ ] `mvn test -DskipTests=false`（exam-admin）全绿
- [ ] 无新增 lint 警告（`ruff check` / `mypy` 暂不强制）
- [ ] OpenAPI 文档可访问 `/v1/docs`

### 9.4 运维验收

- [ ] Docker 镜像 `omr-service:latest` 构建成功
- [ ] `docker compose up -d` 启动后 `curl http://localhost:8080/v1/health` 返回 200
- [ ] Nacos `omr-service` 节点在线，仅显示应用级（无 `providers:omr.OmrService::`）
- [ ] Redis Stream `omr:batch:job` 投递正常
- [ ] 日志中不再出现 `grpcio` / `protobuf` 相关条目
- [ ] `docker-compose.prod.yml` 确认无 RabbitMQ 相关配置（OMR 仅用 Redis Stream；如有 OMR 之外的 RabbitMQ 配置，与本 spec 无关）

### 9.5 文档验收

- [ ] `screenImg/docs/OMR服务接口文档.md` 改写为 HTTP-only 文档
- [ ] `screenImg/AGENTS.md` / `screenImg/CLAUDE.md` 删除 Dubbo 部分
- [ ] `backend/RuoYi-Cloud-Plus/docs/superpowers/specs/2026-06-30-paper-template-design.md` 加 "OMR 改为 HTTP" 修订
- [ ] `screenImg/README.md` 端点对照表更新

## 10. 风险与缓解

| 风险 | 缓解 |
|---|---|
| FastAPI 路由函数若写成 `async def` 会卡事件循环 | Code review 强制 `def`；新增 `tests/api/test_concurrency.py` 验证并发 |
| PaddleOCR 多 worker 显存爆炸 | `--workers 1` 硬性配置；启动脚本检测 |
| Redis Stream 异步任务结果读取延迟 | `omr:batch:result` 写两份 (Stream + Hash)，后者做秒级查询 |
| Nacos 老 `providers:omr.OmrService::` 残留 | 主动 deregister + Nacos 默认 30s 过期 |
| 端口 8080 与 k8s ingress 冲突 | 用 `OMR_HTTP_PORT` 灵活配置 |
| `?wait=false` 半异步语义引起后续困惑 | spec 明确"不做 `?wait=false`"，单轨设计 |
| Java 端 `ExamPaperTemplateServiceImplTest` 改写工作量大 | 单独任务，编号 J-S5 |

## 11. 实施计划（粗）

**注意**：本 spec 涵盖 Python (OMR) + Java (exam-admin) 两端改造。建议拆分为两个独立 plan：
- **Plan A (Python OMR)**：§11 P0-P6, P9 部分
- **Plan B (Java exam-admin)**：§11 P7-P8

两个 plan 可并行实施，但 Plan B 验收前不能合并到 main。

**依赖关系**：P0 → P1 → P2 → P3（启动）→ P4（测试）→ P5（清理）→ P9（回归）；P6（文档）、P7（Java 端）、P8（部署）可与 P3-P5 并行。

详见后续 `superpowers:writing-plans` 产出的 plan 文档。粗粒度拆解：

1. **P0 基础设施**：`pyproject.toml` / `requirements.txt` 加 FastAPI / uvicorn / pytest / httpx；`config.py` 改 Pydantic Settings
2. **P1 业务层**：`core/service.py` 抽离；`core/exceptions.py`；`api/schemas/`
3. **P2 路由层**：`api/routers/recognize.py` / `templates.py` / `tasks.py` / `health.py` / `crops.py`
4. **P3 启动改造**：`main.py` 改 uvicorn；`nacos_reg.py` 清理接口级注册
5. **P4 测试**：`tests/api/` + `tests/core/` 覆盖；保留 `tests/engine/` `tests/mq/` `tests/loader/`
6. **P5 清理**：`engine/processor.py` 删；`rpc/` 删；`http_server.py` 删；`server.py` 删；`health.py` 删
7. **P6 文档同步**：`OMR服务接口文档.md` / `screenImg/AGENTS.md` / `screenImg/CLAUDE.md` / `backend/.../2026-06-30-paper-template-design.md`
8. **P7 Java 端**：`exam-admin` 端 URL 路径 + JSON 字段名迁移；`pom.xml` 清理；`ExamPaperTemplateServiceImplTest` 改写
9. **P8 部署**：`Dockerfile` 改 ENTRYPOINT；`docker-compose.yml` 端口调整；`.env.example` 改字段
10. **P9 回归**：Java exam-admin 单模块跑通；OMR 服务压力测试；E2E 回归

## 12. 开放问题

1. **`omr:batch:result` 是否同时写 Hash 用于秒级查询？** —— 实现阶段确认。推荐"是"，避免 `XREAD` 阻塞。
2. **task_id 格式**：`mq/producer.py` 当前生成策略（待实现阶段确认，建议 `uuid4`）。
3. **H5 / Vben5 是否要直接接 REST 而不是经 exam-admin？** —— 本 spec 不涉及，留作后续 brainstorm。
4. **Dubbo 死代码是否一并删除**（包括 `omr.proto`、`dubbo-maven-plugin`）？—— 推荐"是"，本 spec §4.2 已列。
5. **`mvn test` 阶段是否同步移 Dubbo 依赖**？—— 推荐"是"，但单独做一个迁移任务。

## 13. 变更记录

- 2026-07-28 初稿
- 待用户审阅
