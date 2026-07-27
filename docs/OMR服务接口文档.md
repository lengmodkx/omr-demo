# OMR Python 服务接口文档

> 服务名：`omr-service`  
> 协议：gRPC（兼容 Dubbo Triple）  
> 接口名：`omr.OmrService`  
> proto 文件：`omr_service/rpc/omr.proto`

## 1. 服务发现

### Nacos 注册信息

- **应用级服务名**：`omr-service`
- **接口级服务名**：`providers:omr.OmrService::`
- **分组**：`DEFAULT_GROUP`（默认，可通过 `NACOS_GROUP_NAME` 修改）
- **协议**：`tri`（Dubbo Triple，即 gRPC over HTTP/2）
- **默认端口**：`20884`

### Java / Dubbo 消费端配置

```yaml
dubbo:
  application:
    name: ruoyi-exam-admin
    service-discovery:
      migration: FORCE_INTERFACE   # 强制接口级发现
  registry:
    address: nacos://39.153.154.183:8848
  consumer:
    protocol: tri
    timeout: 10000
```

```java
@DubboReference(version = "1.0.0", group = "DEFAULT_GROUP", protocol = "tri")
private OmrService omrService;
```

## 2. RPC 方法概览

| 方法 | 请求 | 响应 | 说明 |
|------|------|------|------|
| `ParseGoldenTemplate` | `GoldenTemplateRequest` | `GoldenTemplateResult` | 解析黄金模板 |
| `RecognizeByTemplate` | `RecognizeRequest` | `RecognizeResult` | 识别单张答题卡 |
| `VerifyRecognitionRate` | `VerifyRateRequest` | `VerifyRateResult` | 验证模板识别成功率 |
| `ReverifyPaper` | `RecognizeRequest` | `RecognizeResult` | 单张试卷复验（语义层） |

## 3. 消息定义

### ColumnConfig（列框配置）

一个答题卡区域的气泡排列配置，与 `StandardTemplate._generate_grid` 参数一一对应。

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `x1` | int32 | 是 | 列框左上角 x |
| `y1` | int32 | 是 | 列框左上角 y |
| `x2` | int32 | 是 | 列框右下角 x |
| `y2` | int32 | 是 | 列框右下角 y |
| `start_q` | int32 | 是 | 该列起始题号 |
| `num_q` | int32 | 是 | 该列题目数量 |
| `num_options` | int32 | 是 | 每题选项数量，例如 4 |
| `option_axis` | string | 否 | 选项排列方向：`"x"` 竖排题；`"y"` 横排题 |
| `reverse_q` | bool | 否 | 题号是否倒序 |

### GoldenTemplateRequest（解析黄金模板请求）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `template_id` | int64 | 是 | 模板唯一标识，后续识别复用 |
| `template_image_url` | string | 是 | 模板图片 URL（PNG/JPG） |
| `columns` | repeated ColumnConfig | 是 | 列框配置列表，至少一个 |

### GoldenTemplateResult（解析黄金模板响应）

| 字段 | 类型 | 说明 |
|------|------|------|
| `code` | int32 | `0` 成功；非 0 见错误码表 |
| `message` | string | 结果说明 |
| `template_id` | int64 | 模板 ID |
| `bubbles` | repeated Bubble | 检测到的所有气泡 |
| `answers` | map<int32, string> | 标准答案，key 为题号 |
| `total` | int32 | 气泡总数 |

#### Bubble

| 字段 | 类型 | 说明 |
|------|------|------|
| `q` | int32 | 所属题号 |
| `opt` | string | 选项，如 `"A"` |
| `x` | int32 | 气泡中心 x |
| `y` | int32 | 气泡中心 y |
| `w` | int32 | 宽度 |
| `h` | int32 | 高度 |

### RecognizeRequest（识别请求）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `template_id` | int64 | 是 | 已解析的黄金模板 ID |
| `scan_image_url` | string | 是 | 待识别答题卡图片 URL |
| `question_no` | int32 | 否 | `0` 表示整张识别；非 0 为单题复验（预留） |

### RecognizeResult（识别响应）

| 字段 | 类型 | 说明 |
|------|------|------|
| `code` | int32 | `0` 成功；非 0 见错误码表 |
| `message` | string | 结果说明 |
| `template_id` | int64 | 模板 ID |
| `scan_image_url` | string | 被识别的图片 URL |
| `answers` | repeated QuestionAnswer | 每题识别结果 |
| `total` | int32 | 总题数 |
| `empty_count` | int32 | 空选数量 |
| `multi_count` | int32 | 多选数量 |
| `card_flag` | string | 异常标记：`abnormal` / `suspicious_blank` / `invalid_image` / `""` |
| `duration_ms` | int32 | 识别耗时（毫秒） |

#### QuestionAnswer

| 字段 | 类型 | 说明 |
|------|------|------|
| `q` | int32 | 题号 |
| `answer` | string | 识别答案，单选 `"A"`、多选 `"ABC"`、空 `""` |
| `status` | string | `single` / `multi` / `empty` / `uncertain` |
| `correct` | bool | 是否正确（复验/成功率验证场景使用） |

### VerifyRateRequest（验证成功率请求）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `template_id` | int64 | 是 | 模板 ID |
| `image_urls` | repeated string | 是 | 已知答案的样本图片 URL 列表 |
| `expected_answers` | map<int32, string> | 是 | 标准答案，key 为题号，value 为 `"A"` 或 `"ABC"` 等 |

### VerifyRateResult（验证成功率响应）

| 字段 | 类型 | 说明 |
|------|------|------|
| `code` | int32 | `0` 成功 |
| `message` | string | 结果说明 |
| `success_rate` | float | 成功率 `0.0 ~ 1.0` |
| `total` | int32 | 样本总数 |
| `matched` | int32 | 匹配数 |
| `details` | repeated RecognizeResult | 每张样本的详细识别结果 |

## 4. 错误码

| 错误码 | 含义 | 常见原因 |
|--------|------|----------|
| `0` | 成功 | - |
| `4` | 模板未找到 | `template_id` 未调用 `ParseGoldenTemplate` 解析 |
| `5` | 图片加载失败 | URL 无效、OSS 权限、图片过大/损坏 |
| `6` | 请求参数非法 | `template_id` 为 0、URL 为空、`columns` 为空等 |
| `99` | 内部错误 | 识别异常，需查看服务端日志 |

## 5. 调用示例

### 5.1 Java / Dubbo

```java
// 1. 解析黄金模板
GoldenTemplateRequest tplReq = GoldenTemplateRequest.newBuilder()
    .setTemplateId(1001L)
    .setTemplateImageUrl("https://oss/template.jpg")
    .addColumns(ColumnConfig.newBuilder()
        .setX1(100).setY1(200).setX2(300).setY2(800)
        .setStartQ(1).setNumQ(5).setNumOptions(4)
        .setOptionAxis("x")
        .build())
    .build();
GoldenTemplateResult tplRes = omrService.parseGoldenTemplate(tplReq);

// 2. 识别答题卡
RecognizeRequest recReq = RecognizeRequest.newBuilder()
    .setTemplateId(1001L)
    .setScanImageUrl("https://oss/scan.jpg")
    .build();
RecognizeResult recRes = omrService.recognizeByTemplate(recReq);
```

### 5.2 Python 直连

```python
import grpc
from omr_service.rpc import omr_pb2, omr_pb2_grpc

channel = grpc.insecure_channel("192.168.31.229:20884")
stub = omr_pb2_grpc.OmrServiceStub(channel)

req = omr_pb2.RecognizeRequest(
    template_id=1001,
    scan_image_url="https://oss/scan.jpg",
)
resp = stub.RecognizeByTemplate(req)
print(resp)
```

### 5.3 Python Tag 感知客户端（推荐本地调试）

```bash
python -m omr_service.rpc.tag_aware_client \
    --method RecognizeByTemplate \
    --tag zhangsan \
    --template-id 1001 \
    --image-url "https://oss/scan.jpg"
```

## 6. Tag 路由隔离（本地调试）

多人共用同一 Nacos 时，为避免请求打到他人本地实例：

1. 本地启动前设置 Tag：
   ```powershell
   $env:OMR_SERVICE_TAG="zhangsan"
   python -m omr_service.main
   ```
2. Java 消费端调用时设置：
   ```java
   RpcContext.getContext().setAttachment("dubbo.tag", "zhangsan");
   ```
3. Python 消费端调用时在 metadata 中携带：
   ```python
   stub.RecognizeByTemplate(req, metadata=[("x-service-tag", "zhangsan")])
   ```

Provider 注册时会在 Nacos 元数据中写入 `tag=zhangsan` 和 `dubbo.tag=zhangsan`。消费端优先选择相同 Tag 的实例，未命中则 fallback 到空 Tag 的基线实例。

## 7. 健康检查

- **地址**：`http://<ip>:9173/health`
- **方法**：`GET`
- **成功响应**：`{"status":"ok"}`

## 8. Redis Stream 批量任务（可选）

除了 gRPC 单张调用，也支持通过 Redis Stream 批量下发任务。

### 任务消息格式（admin → omr-service）

发往 Stream：`omr:batch:job`

```json
{
  "job_id": "uuid",
  "template_id": 1001,
  "image_urls": ["https://oss/01A.jpg", "https://oss/02A.jpg"],
  "result_stream": "omr:batch:result"
}
```

### 结果消息格式（omr-service → admin）

写回 Stream：`omr:batch:result`

```json
{
  "job_id": "uuid",
  "template_id": 1001,
  "completed": 2,
  "failed": 0,
  "results": [
    {"scan_image_url": "...", "answers": [...], "code": 0}
  ]
}
```
