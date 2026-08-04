# OMR Python 服务

答题卡智能识别系统的 Python 微服务实现，替代原 Streamlit Demo 与 Go 服务方向。

## 架构

```
                        Nacos
                    （注册中心 + 配置中心）
                         ▲
exam-admin (Java)        │ heartbeat
     │ HTTP              │
     ▼                   │
omr-service(Python) :8080
     │
     │ HTTP GET
     ▼
   OSS / 图片 URL

     ◄── Redis Stream ──►
  批量任务下发 / 结果回传
```

## 核心能力

| 端点 | 用途 | 调用方 |
|------|------|--------|
| `POST /v1/recognize` | 同步识别 | admin 收学生答卷时 |
| `POST /v1/templates/parse` | 同步模板解析 | admin 发布模板时 |
| `POST /v1/reverify_paper` | 复验 | admin 人工复核时 |
| `POST /v1/tasks` | 异步任务投递 | admin 批量识别 |
| `GET /v1/tasks/{task_id}` | 异步任务查询 | admin 查询任务状态 |
| `GET /v1/health` | 存活 | Nacos / LB |
| `GET /v1/health/ready` | 就绪（含 Redis 连通性检查） | Nacos / LB |
| `GET /v1/omr_crops/{file_path}` | 静态裁剪图 | admin 取回裁剪结果 |
| `GET /v1/docs` | Swagger UI | 调试 |
| `GET /v1/openapi.json` | OpenAPI 文档 | 客户端生成 |

## 快速开始

### 前置条件

- Python 3.11（PaddleOCR 兼容性要求）
- Nacos 服务（注册中心 + 配置中心）
- Redis（消息队列 + 任务状态存储）

### 1. 安装依赖

```bash
cd screenImg

# Windows
.venv-py311\Scripts\Activate.ps1

# Linux/macOS
python3.11 -m venv .venv
source .venv/bin/activate

# 分两步安装（paddlepaddle 和 nacos-sdk-python 对 protobuf 版本要求冲突）
pip install -r requirements.txt
pip install --no-deps -r requirements-nacos.txt

# 修复 nacos-sdk-python 与 PaddleOCR 的 protobuf 兼容性
python scripts/patch_nacos_protobuf.py
```

### 2. 配置

配置来源优先级：**Nacos 配置中心 > 本地环境变量（.env）> 默认值**

> ⚠️ **安全注意**：所有敏感信息（Nacos 用户名/密码、Redis 密码、服务器地址）必须通过环境变量或 Nacos 配置中心注入，**禁止硬编码在代码中**。

#### 方式一：本地环境变量（推荐开发环境）

```bash
# 从模板创建
cp .env.example .env

# 编辑 .env，填入实际的 Nacos / Redis 地址和凭证
# 详细变量说明见下方"环境变量"章节
```

**.env.example 示例**：

```bash
# 服务监听
OMR_HTTP_HOST=0.0.0.0
OMR_HTTP_PORT=8080
OMR_LOG_LEVEL=INFO

# Nacos（启用/禁用开关 + 连接信息）
OMR_NACOS_ENABLED=true
OMR_NACOS_SERVER=127.0.0.1:8848
OMR_NACOS_NAMESPACE=public
OMR_NACOS_GROUP=DEFAULT_GROUP
OMR_NACOS_DATA_ID=omr-service.yaml
OMR_NACOS_SERVICE_NAME=omr-service

# Redis（启用/禁用开关 + 连接信息）
OMR_REDIS_ENABLED=true
OMR_REDIS_HOST=127.0.0.1
OMR_REDIS_PORT=6379
OMR_REDIS_DB=1
OMR_REDIS_PASSWORD=your_redis_password

# Redis Stream（可选，默认值通常不需要改）
OMR_REDIS_STREAM_JOB=omr:batch:job
OMR_REDIS_STREAM_RESULT=omr:batch:result
OMR_REDIS_RESULT_HASH_PREFIX=omr:batch:result:hash

# 批量任务消费者开关
OMR_CONSUMER_ENABLED=true
OMR_WORKER_POOL_SIZE=4
OMR_SYNC_TIMEOUT_SECONDS=60

# OMR 内部
OMR_TEMPLATE_TTL_SECONDS=3600
OMR_IMAGE_MAX_BYTES=52428800
OMR_CROP_OUTPUT_DIR=./output
OMR_CROP_BASE_URL=http://127.0.0.1:8080/v1/omr_crops
```

#### 方式二：Nacos 配置中心（推荐生产环境）

在 Nacos 控制台创建配置：
- `dataId`: `omr-service.yaml`
- `group`: `DEFAULT_GROUP`
- `namespace`: 与 `OMR_NACOS_NAMESPACE` 一致

示例内容：

```yaml
nacos_server: your-nacos-host:8848
nacos_namespace: your-namespace-id
redis:
  host: your-redis-host
  port: 6379
  password: your_redis_password
  db: 1
omr_worker_count: 4
```

> Nacos 配置中的值优先级高于 `.env` 文件中的同名变量。如果某个变量在 Nacos 中已配置，`.env` 中的值会被覆盖。

### 3. 启动服务

```bash
# 确保已激活虚拟环境且配置好 .env 后
python -m omr_service.main
```

启动后：

- FastAPI HTTP 监听 `0.0.0.0:8080`（可通过 `OMR_HTTP_HOST` / `OMR_HTTP_PORT` 修改）
- Swagger UI：`http://localhost:8080/v1/docs`
- 健康检查：`GET http://localhost:8080/v1/health`
  - `/v1/health` — 存活检查（始终返回 200）
  - `/v1/health/ready` — 就绪检查（含 Redis ping，失败返回 503）
- Nacos 服务列表出现 `omr-service`（HTTP 协议），前提 `OMR_NACOS_ENABLED=true`
- Redis Stream 消费者自动启动，监听 `omr:batch:job`，结果写入 `omr:batch:result`（前提 `OMR_REDIS_ENABLED=true` + `OMR_CONSUMER_ENABLED=true`）

**关闭服务**：按 `Ctrl+C`。服务会自动：

1. 从 Nacos 注销实例（防止脏注册残留）
2. 等待 Redis Stream 消费者排空（最多 30s）
3. 关闭 Worker 线程池

### 4. 验证服务

```bash
# 检查存活
curl http://localhost:8080/v1/health

# 检查就绪（含 Redis 状态）
curl http://localhost:8080/v1/health/ready

# 解析黄金模板
curl -X POST http://localhost:8080/v1/templates/parse \
  -H "Content-Type: application/json" \
  -d '{
    "template_id": 1001,
    "template_image_url": "https://your-oss/template.jpg",
    "columns": [{
      "x1": 100, "y1": 200, "x2": 300, "y2": 800,
      "start_q": 1, "num_q": 5, "num_options": 4,
      "option_axis": "x"
    }]
  }'

# 识别答题卡（template_id 为整数）
curl -X POST http://localhost:8080/v1/recognize \
  -H "Content-Type: application/json" \
  -d '{
    "template_id": 1001,
    "scan_image_urls": ["https://your-oss/scan.jpg"]
  }'
```

> ⚠️ **注意**：`template_id` 传整数或数字字符串均可（服务端自动兼容转换），但响应中统一返回整数。

## 请求字段兼容性约定

为方便 Java 端多链路调用，请求字段同时兼容以下命名（服务端统一归一化为 snake_case）：

- **columns（选择题列）**：`start_q`/`startQ`/`question_start`、`num_q`/`numQ`/`question_count`、`num_options`/`numOptions`/`options_per_question`、`option_axis`/`optionAxis`、`reverse_q`/`reverseQ`、`page_index`/`pageIndex`；坐标支持 `x1/y1/x2/y2` 或 `x/y/width/height`。缺坐标或 `num_q<=0` 的列会被跳过并记 WARNING，不再报错。
- **columns 可以为空**：多页模板中只有个人信息/主观题的页（如第 2 页）没有选择题列，传空数组即可。
- **personal_info_region / subjective_regions**：支持 `pageIndex`→`page_index`、`stitchWithNext`→`stitch_with_next` 驼峰转换。

## 多页答题卡处理约定

Java 端按页拆分请求（每页一个请求、只传该页图片），但区域配置中的 `page_index` 仍是**原始页码**（0 起）。服务端行为：

- 单图请求中，`page_index` 自动归一化为 0 选图；返回结果（主观题裁切）再还原为原始页码；
- 考生信息区（`student_info_block`）整块 OCR 后，解析出的子字段（name/room/seat/exam_no/school 等）会以平铺条目追加返回；
- 模板缓存按真实页码存储各页参考图（`page_images`），多页解析互不覆盖。

## 安全防护

服务内置以下安全措施（2026-07-29 安全审查后新增）：

| 防护项 | 说明 |
|--------|------|
| **SSRF 防护** | 图片 URL 仅允许 `http/https` scheme，禁止内网地址（localhost、127.0.0.1、10.x、192.168.x、172.16-31.x） |
| **路径穿越防护** | crops 静态文件路由使用 `Path.relative_to()` 替代字符串前缀匹配，防止目录穿越 |
| **日志注入防护** | `X-Request-ID` header 校验 `^[A-Za-z0-9_\-]{1,64}$` 正则，非法值自动替换为 UUID |
| **敏感信息脱敏** | 异常消息中的图片 URL 自动去除 query string（避免签名 token 泄露）；OCR 识别结果中的个人信息不再记录到 WARNING 日志 |
| **凭证外部化** | Nacos 用户名/密码、Redis 密码等敏感配置必须通过环境变量注入，代码和脚本中无硬编码凭证 |

## 环境变量

以下变量可写在 `.env` 文件或直接在 shell 中 export。带 `OMR_` 前缀的变量优先匹配新的 `OmrSettings` 配置模型。

### 服务基础

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `OMR_HTTP_HOST` | `0.0.0.0` | 监听地址 |
| `OMR_HTTP_PORT` | `8080` | 监听端口 |
| `OMR_LOG_LEVEL` | `INFO` | 日志级别（DEBUG/INFO/WARNING/ERROR） |

### Nacos

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `OMR_NACOS_ENABLED` | `true` | 是否启用 Nacos 注册+配置（`false` 则完全跳过） |
| `OMR_NACOS_SERVER` | `127.0.0.1:8848` | Nacos 服务器地址 |
| `OMR_NACOS_NAMESPACE` | `public` | Nacos 命名空间 ID |
| `OMR_NACOS_GROUP` | `DEFAULT_GROUP` | Nacos 分组 |
| `OMR_NACOS_DATA_ID` | `omr-service.yaml` | Nacos 配置 dataId |
| `OMR_NACOS_SERVICE_NAME` | `omr-service` | 注册到 Nacos 的服务名 |
| `OMR_NACOS_IP` | 空（自动检测） | 注册时上报的 IP，留空则自动获取本机 IP |

> ⚠️ `NACOS_SERVER`、`NACOS_USERNAME`、`NACOS_PASSWORD` 等旧格式环境变量仅用于旧的 `OmrConfig` dataclass（过渡期保留）。新配置请使用 `OMR_NACOS_*` 前缀变量。Nacos 用户名/密码通过 Nacos 配置中心的 YAML 注入（`nacos_username` / `nacos_password` 字段）。

### Redis

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `OMR_REDIS_ENABLED` | `true` | 是否启用 Redis（`false` 则仅提供 HTTP 同步接口） |
| `OMR_REDIS_HOST` | `127.0.0.1` | Redis 主机 |
| `OMR_REDIS_PORT` | `6379` | Redis 端口 |
| `OMR_REDIS_DB` | `1` | Redis 数据库编号 |
| `OMR_REDIS_PASSWORD` | 空 | Redis 密码 |
| `OMR_REDIS_STREAM_JOB` | `omr:batch:job` | 批量任务 Stream key |
| `OMR_REDIS_STREAM_RESULT` | `omr:batch:result` | 结果输出 Stream key |
| `OMR_REDIS_RESULT_HASH_PREFIX` | `omr:batch:result:hash` | 任务状态 Hash 前缀 |

### Worker / 消费者

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `OMR_CONSUMER_ENABLED` | `true` | 是否启动 Redis Stream 消费者线程 |
| `OMR_WORKER_POOL_SIZE` | `4` | 线程池大小（并行识别数） |
| `OMR_SYNC_TIMEOUT_SECONDS` | `60.0` | 同步识别超时（秒） |
| `OMR_CONSUMER_TASK_TIMEOUT_SEC` | `120` | MQ 单任务处理超时（秒），超时保留 pending 等待重试 |
| `OMR_OCR_TIMEOUT_SECONDS` | `30.0` | 个人信息 OCR / 主观题裁剪单步超时（秒），超时跳过该步不阻塞任务 |
| `OMR_OCR_CONFIDENCE_THRESHOLD` | `0.3` | 个人信息 OCR 置信度阈值，低于阈值视为未识别（value 置空） |

### OMR 引擎

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `OMR_TEMPLATE_TTL_SECONDS` | `3600` | 模板缓存 TTL（秒） |
| `OMR_IMAGE_MAX_BYTES` | `52428800` | 图片最大字节数（50MB） |
| `OMR_CROP_OUTPUT_DIR` | `./output` | 主观题裁剪输出目录 |
| `OMR_CROP_BASE_URL` | 空 | 裁切图外部访问 URL 前缀，如 `http://host:8080/v1/omr_crops`。**必须配置**，否则裁切图链接为空 |

### 兼容期字段（1 个版本后删除）

| 变量 | 说明 |
|------|------|
| `OMR_DUBBO_PORT` | 已废弃。Dubbo Triple 服务已下线，请使用 `OMR_HTTP_PORT` |
| `OMR_HEALTH_PORT` | 已废弃。健康检查已合并到 `OMR_HTTP_PORT` |
| `OMR_LEGACY_DUBBO_PORT` | 已废弃。留空即可 |

## 本地调试隔离（Service Tag）

多人共用同一 Nacos 注册中心时，为避免请求打到其他开发者的本地实例：

```bash
# 方式一：用 OMR_NACOS_IP 绑定本机 IP
OMR_NACOS_IP=192.168.1.100 python -m omr_service.main

# 方式二：用独立的 Nacos namespace 隔离
OMR_NACOS_NAMESPACE=dev-zhangsan python -m omr_service.main
```

## 接口文档

完整 HTTP 接口定义、字段说明、curl / Python 调用示例见：

📄 [`docs/OMR服务接口文档.md`](docs/OMR服务接口文档.md)

## Redis Stream 批量任务

消费者监听 `omr:batch:job`，结果写回 `omr:batch:result`，支持两种任务类型：

**① 单张答卷识别任务（admin → omr-service）**：

```json
{
  "task_id": "uuid",
  "batch_id": "批次ID",
  "paper_id": 123,
  "template_id": 1001,
  "image_url": "https://oss/xxx/01A.jpg",
  "retry_count": 0,
  "max_retry": 3
}
```

**② 黄金模板解析任务（`job_type=parse_golden_template`，按页拆分）**：

```json
{
  "job_type": "parse_golden_template",
  "job_id": "uuid",
  "template_id": 1001,
  "pages": [
    {
      "pageIndex": 0,
      "templateImageUrl": "https://oss/xxx/page1.jpg",
      "columns": [{"x1": 100, "y1": 200, "x2": 300, "y2": 800, "startQ": 1, "numQ": 5, "numOptions": 4, "optionAxis": "x", "reverseQ": false, "pageIndex": 0}],
      "personalInfo": [{"field": "student_info_block", "x1": 0, "y1": 0, "x2": 100, "y2": 50, "pageIndex": 0}],
      "subjectiveRegions": [{"q": 51, "x1": 0, "y1": 0, "x2": 100, "y2": 50, "pageIndex": 0, "stitchWithNext": false}]
    }
  ]
}
```

**结果消息（omr-service → admin）**：识别任务回写 `{task_id, status, answers, personal_info, subjective_crops, ...}`（`status=2` 成功 / `3` 失败）；模板解析回写 `{job_id, job_type, status, answers(map), bubbles, personal_info, subjective_crops}`。永久性错误（模板不存在/图片加载失败）不重试，其余错误自动重试至 `max_retry`。

## 目录结构

```
screenImg/
├── omr_service/              # Python 微服务
│   ├── main.py               # 服务入口（uvicorn + FastAPI lifespan）
│   ├── config.py             # 配置加载（Nacos + env）
│   ├── nacos_config.py       # Nacos 配置中心客户端（gRPC）
│   ├── nacos_reg.py          # Nacos 服务注册
│   ├── nacos_v2_compat.py    # nacos-sdk-python v2 兼容性补丁
│   ├── api/                  # FastAPI 应用工厂 + 路由 + Pydantic schema
│   │   ├── app.py            # FastAPI 应用创建 + X-Request-ID 中间件
│   │   ├── deps.py           # 依赖注入
│   │   ├── errors.py         # 错误处理
│   │   ├── routers/          # 路由模块（health/recognize/templates/tasks/crops）
│   │   └── schemas/          # Pydantic 请求/响应模型
│   ├── core/                 # OmrService + TaskRegistry + exceptions
│   ├── mq/                   # Redis Stream 生产/消费
│   │   ├── client.py         # Redis 客户端工厂
│   │   ├── consumer.py       # Stream 消费者（后台线程）
│   │   ├── producer.py       # Stream 生产者
│   │   └── job_handler.py    # 任务处理器
│   ├── engine/               # OMR 识别引擎
│   │   ├── recognizer.py     # 识别器
│   │   ├── cropper.py        # 主观题裁剪
│   │   ├── ocr.py            # PaddleOCR 个人信息识别
│   │   ├── score_calculator.py
│   │   ├── standard_template.py
│   │   └── personal_info_block_parser.py
│   ├── loader/               # 图片加载 + 模板缓存
│   ├── worker/               # 线程池
│   ├── scripts/              # 运维脚本
│   └── benchmarks/           # 性能基准测试
├── omr_demo/                 # 原 Demo 脚本（已移除 Streamlit UI）
├── testPaper/                # 样例答题卡图片
├── .env.example              # 环境变量模板
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── docker-compose.prod.yml
```

## Docker 部署

```bash
# 开发环境
docker compose build
docker compose up -d

# 生产环境（使用 docker-compose.prod.yml）
docker compose -f docker-compose.prod.yml build
docker compose -f docker-compose.prod.yml up -d
```

**Docker 环境变量**：通过 `docker-compose.yml` 中的 `environment` 段或 `env_file` 指定 `.env` 文件注入。

## 测试

```bash
# 单元测试（pytest，tests/ 目录，见 pytest.ini）
python -m pytest -v
```

## 故障排查

### 启动失败：`ModuleNotFoundError: No module named 'omr_service'`

确保在 `screenImg/` 目录下运行，且虚拟环境已激活。`omr_service` 没有注册为 pip 包，依赖 Python 的隐式包发现机制。

### 启动失败：`ImportError: get_redis_client`

确认 `omr_service/mq/client.py` 文件完整。如果 Redis 不需要，可设置 `OMR_REDIS_ENABLED=false` 跳过 Redis 初始化。

### 启动失败：Nacos 连接超时

- 检查 `OMR_NACOS_SERVER` 地址是否正确
- 检查网络连通性：`curl http://<nacos-host>:8848/nacos/v1/console/health`
- 如果不需要 Nacos，设置 `OMR_NACOS_ENABLED=false` 跳过

### 启动失败：Redis 连接超时

- 检查 `OMR_REDIS_HOST` / `OMR_REDIS_PORT` / `OMR_REDIS_PASSWORD` 配置
- 检查网络连通性：`redis-cli -h <host> -p <port> -a <password> ping`
- 如果不需要 Redis（仅用 HTTP 同步接口），设置 `OMR_REDIS_ENABLED=false`

### `/v1/health/ready` 返回 503

表示 Redis 连接失败或未配置。检查 Redis 相关环境变量。如果未启用 Redis（`OMR_REDIS_ENABLED=false`），就绪检查不会检测 Redis。

### 裁剪图链接为空

在 `.env` 中配置 `OMR_CROP_BASE_URL` 为完整的外部可访问 URL 前缀，例如：

```bash
OMR_CROP_BASE_URL=http://your-server:8080/v1/omr_crops
```

### Nacos 实例残留

关闭服务时请使用 `Ctrl+C`（不要用 `kill -9`），服务会自动注销 Nacos。如已有脏注册残留，可手动在 Nacos 控制台删除，或等待 TTL 自动过期。

### PaddleOCR / protobuf 版本冲突

```text
TypeError: Descriptors cannot be created directly
```

运行兼容性补丁：

```bash
python scripts/patch_nacos_protobuf.py
```

### 选择题识别全部错误 / 裁切位置明显偏移

首先核对**框选坐标与图片实际像素是否一致**：坐标必须是以原图像素为基准的绝对坐标。前端框选页曾在图片被 CSS 缩小显示时按缩小后的比例记录坐标（已修复），历史数据若按比例整体偏移（如都是正确值的 0.88 倍），需要在页面上重新框选。排查技巧：把气泡网格（`/v1/templates/parse` 返回的 `bubble_grid`）画到模板图上肉眼比对。

### 多页模板第 2 页裁切不出来

2026-08-04 之前的版本丢失了“单图请求 page_index 归一化”逻辑，升级到最新代码后重新触发模板解析即可。

### 个别答题卡个人信息识别为空（其余正常）

PaddleOCR 推理引擎非线程安全，旧版本多 worker 并发调用会偶发返回空。最新代码已对推理调用串行化；如仍出现，检查服务日志中的 `考生信息区 OCR 异常` 警告。
