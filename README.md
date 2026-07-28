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
| `GET /v1/health/ready` | 就绪 | Nacos / LB |
| `GET /v1/omr_crops/{file_path}` | 静态裁剪图 | admin 取回裁剪结果 |
| `GET /v1/docs` | Swagger UI | 调试 |
| `GET /v1/openapi.json` | OpenAPI 文档 | 客户端生成 |

## 快速开始

启动后：
- FastAPI HTTP 监听 `:8080`
- 健康检查 `GET http://<ip>:8080/v1/health`
- Swagger UI：`http://<ip>:8080/v1/docs`
- Nacos 服务列表应出现 `omr-service`（HTTP 协议）
- Redis Stream `omr:batch:job` 接收批量任务，`omr:batch:result` 输出结果

### 1. 安装依赖

```bash
python -m venv .venv
source .venv/Scripts/activate  # Windows
. .venv-py311/Scripts/Activate.ps1
# source .venv/bin/activate    # Linux/macOS
pip install -r requirements.txt
```

### 2. 配置

配置来源优先级：**Nacos 配置中心 > 本地环境变量 > 默认值**

#### 方式一：Nacos 配置中心（推荐）

在 Nacos 控制台创建配置：
- `dataId`: `omr-service.yaml`
- `group`: `DEFAULT_GROUP`
- `namespace`: `8c4541fd-870e-414d-bdee-72cab49fe8d2`
- 示例内容：

```yaml
nacos_server: 39.153.154.183:8848
nacos_namespace: 8c4541fd-870e-414d-bdee-72cab49fe8d2
nacos_username: nacos
nacos_password: ***REMOVED***
redis:
  host: 47.99.83.217
  port: 6379
  password: ***REMOVED***
  db: 4
omr_worker_count: 4
```

> 也可直接导入 `nacoss-config-example.yaml`。

#### 方式二：本地环境变量

```bash
cp .env.example .env
# 编辑 .env，填入实际 Nacos / Redis 地址
```

### 3. 启动服务

```bash
python -m omr_service.main
# 等价于：
# uvicorn omr_service.main:app --host 0.0.0.0 --port 8080
```

## 接口文档

完整 HTTP 接口定义、字段说明、curl / Python 调用示例见：

📄 [`docs/OMR服务接口文档.md`](docs/OMR服务接口文档.md)

## 消费端调用示例（HTTP）

```bash
# 解析黄金模板
curl -X POST http://omr-service:8080/v1/templates/parse \
  -H "Content-Type: application/json" \
  -d '{
    "template_id": 1001,
    "template_image_url": "https://oss/template.jpg",
    "columns": [{
      "x1": 100, "y1": 200, "x2": 300, "y2": 800,
      "start_q": 1, "num_q": 5, "num_options": 4,
      "option_axis": "x"
    }]
  }'

# 识别答题卡
curl -X POST http://omr-service:8080/v1/recognize \
  -H "Content-Type: application/json" \
  -d '{
    "template_id": 1001,
    "scan_image_url": "https://oss/scan.jpg"
  }'
```

## 目录结构

```
.
├── omr_service/              # Python 微服务
│   ├── main.py               # 服务入口（启动 uvicorn + daemon 线程）
│   ├── config.py             # 配置加载（Nacos + env）
│   ├── nacos_config.py       # Nacos 配置中心客户端（gRPC）
│   ├── nacos_reg.py          # Nacos 服务注册
│   ├── nacos_v2_compat.py    # nacos-sdk-python v2 兼容性补丁
│   ├── api/                  # FastAPI 应用工厂 + 路由 + Pydantic schema
│   ├── core/                 # OmrService + TaskRegistry
│   ├── mq/                   # Redis Stream 生产/消费
│   ├── engine/               # OMR 识别引擎
│   ├── loader/               # 图片加载 + 模板缓存
│   └── worker/               # 线程池
├── omr_demo/                 # 原 Demo 脚本（已移除 Streamlit UI）
├── testPaper/                # 样例答题卡图片
├── requirements.txt
├── Dockerfile
└── docker-compose.yml
```

## Docker 部署

```bash
docker compose build
docker compose up -d
```

## 测试

```bash
source .venv/Scripts/activate
python -m unittest discover -s tests -p "test_*.py" -v
```

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `OMR_HTTP_HOST` | `0.0.0.0` | uvicorn 监听地址 |
| `OMR_HTTP_PORT` | `8080` | uvicorn 监听端口 |
| `NACOS_SERVER` | `127.0.0.1:8848` | Nacos 地址 |
| `NACOS_NAMESPACE` | `public` | Nacos 命名空间 |
| `NACOS_USERNAME` | - | Nacos 用户名 |
| `NACOS_PASSWORD` | - | Nacos 密码 |
| `NACOS_CONFIG_DATA_ID` | `omr-service.yaml` | Nacos 配置 dataId |
| `REDIS_HOST` | `127.0.0.1` | Redis 主机 |
| `REDIS_PORT` | `6379` | Redis 端口 |
| `REDIS_PASSWORD` | - | Redis 密码 |
| `REDIS_DB` | `4` | Redis 数据库 |
| `REDIS_JOB_STREAM` | `omr:batch:job` | 批量任务 Stream |
| `REDIS_RESULT_STREAM` | `omr:batch:result` | 结果输出 Stream |
| `OMR_WORKER_COUNT` | CPU 核数 | 并发 worker |
| `OMR_SERVICE_TAG` | - | 服务实例 Tag，用于本地调试隔离（空值为基线实例） |

## 本地调试隔离（Service Tag）

多人共用同一 Nacos 注册中心时，为避免请求打到其他开发者的本地实例，可给实例打 Tag：

```bash
# 开发者 A 本地启动
OMR_SERVICE_TAG=zhangsan python -m omr_service.main

# 开发者 B 本地启动
OMR_SERVICE_TAG=lisi python -m omr_service.main
```

Provider 注册时会在 Nacos metadata 中写入 `tag`。

**HTTP 消费端**通过 Header 携带 Tag：

```bash
curl -H "x-service-tag: zhangsan" \
     -X POST http://omr-service:8080/v1/recognize \
     -H "Content-Type: application/json" \
     -d '{"template_id": 1, "scan_image_url": "https://oss/xxx.jpg"}'
```

> 测试环境建议常驻至少一个空 Tag 基线实例，避免未带 Tag 的请求无实例可用。

## Redis Stream 批量任务

**任务消息格式（admin → omr-service）**：

```json
{
  "job_id": "uuid",
  "template_id": 1001,
  "image_urls": ["https://oss/xxx/01A.jpg"],
  "result_stream": "omr:batch:result"
}
```

**结果消息格式（omr-service → admin）**：

```json
{
  "job_id": "uuid",
  "template_id": 1001,
  "completed": 1,
  "failed": 0,
  "results": [{"scan_image_url": "...", "answers": [...], "code": 0}]
}
```

Java 端可用 `RedisTemplate.opsForStream()` 或 `StreamListener` 读写。