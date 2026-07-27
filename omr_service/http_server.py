"""HTTP wrapper around Dubbo Tri OMR service.

Dubbo 3 + Python omr-client (nacos-sdk-python) 之间的 service-discovery / metadata
过滤链当前不稳定。为了让业务 (recognizeAll 接口) 能跑通,我们在 omr-service 内
启动一个简版 HTTP server,把 HTTP JSON 请求转发到现有的 OmrServiceServicer。

HTTP endpoints:
    POST /parse_golden_template    body: GoldenTemplateRequest JSON -> GoldenTemplateResult JSON
    POST /recognize_by_template    body: RecognizeRequest JSON      -> RecognizeResult JSON
    POST /reverify_paper           body: RecognizeRequest JSON      -> RecognizeResult JSON
    GET  /health

JSON structure follows protobuf JSON mapping (字段名同 protobuf).
"""
from __future__ import annotations

import json
import logging
import mimetypes
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import os
import sys

# 让 import omr_service.* 时能找到
_PKG_PARENT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from omr_service.rpc.omr_pb2 import (  # noqa
    GoldenTemplateRequest,
    GoldenTemplateResult,
    RecognizeRequest,
    RecognizeResult,
    VerifyRateRequest,
    VerifyRateResult,
    ColumnConfig,
    PersonalInfoConfig,
    SubjectiveRegion,
    RegionType,
)
from omr_service.rpc.omr_service import OmrServiceServicer  # noqa


logger = logging.getLogger(__name__)


# Handlers --------------------------------------------------------------

def _make_column_config_pb(j: Dict[str, Any]) -> Any:
    pb = ColumnConfig()
    if "x1" in j: pb.x1 = int(j["x1"])
    if "y1" in j: pb.y1 = int(j["y1"])
    if "x2" in j: pb.x2 = int(j["x2"])
    if "y2" in j: pb.y2 = int(j["y2"])
    if "startQ" in j: pb.start_q = int(j["startQ"])
    if "numQ" in j: pb.num_q = int(j["numQ"])
    if "numOptions" in j: pb.num_options = int(j["numOptions"])
    if "optionAxis" in j: pb.option_axis = j["optionAxis"]
    if "reverseQ" in j: pb.reverse_q = bool(j["reverseQ"])
    if "regionType" in j:
        pb.region_type = RegionType.Value(j["regionType"]) if isinstance(j["regionType"], str) else int(j["regionType"])
    if "regionMeta" in j: pb.region_meta = j["regionMeta"]
    if "pageIndex" in j: pb.page_index = int(j["pageIndex"])
    return pb


def _make_personal_info_config_pb(j: Dict[str, Any]) -> Any:
    pb = PersonalInfoConfig()
    if "field" in j: pb.field = j["field"]
    if "x1" in j: pb.x1 = int(j["x1"])
    if "y1" in j: pb.y1 = int(j["y1"])
    if "x2" in j: pb.x2 = int(j["x2"])
    if "y2" in j: pb.y2 = int(j["y2"])
    if "pageIndex" in j: pb.page_index = int(j["pageIndex"])
    return pb


def _make_subjective_region_pb(j: Dict[str, Any]) -> Any:
    pb = SubjectiveRegion()
    if "q" in j: pb.q = int(j["q"])
    if "x1" in j: pb.x1 = int(j["x1"])
    if "y1" in j: pb.y1 = int(j["y1"])
    if "x2" in j: pb.x2 = int(j["x2"])
    if "y2" in j: pb.y2 = int(j["y2"])
    if "pageIndex" in j: pb.page_index = int(j["pageIndex"])
    if "stitchWithNext" in j: pb.stitch_with_next = bool(j["stitchWithNext"])
    return pb


def _make_golden_template_request(j: Dict[str, Any]) -> Any:
    req = GoldenTemplateRequest()
    if "templateId" in j: req.template_id = int(j["templateId"])
    if "templateImageUrl" in j: req.template_image_url = j["templateImageUrl"]
    if "columns" in j:
        for c in j["columns"]:
            req.columns.append(_make_column_config_pb(c))
    if "personalInfo" in j:
        for p in j["personalInfo"]:
            req.personal_info.append(_make_personal_info_config_pb(p))
    if "subjectiveRegions" in j:
        for s in j["subjectiveRegions"]:
            req.subjective_regions.append(_make_subjective_region_pb(s))
    return req


def _make_recognize_request(j: Dict[str, Any]) -> Any:
    req = RecognizeRequest()
    if "templateId" in j: req.template_id = int(j["templateId"])
    if "scanImageUrl" in j: req.scan_image_url = j["scanImageUrl"]
    return req


# TODO: VerifyRecognitionRate 暂未通过 HTTP 暴露，保留构造函数供后续启用。
def _make_verify_rate_request(j: Dict[str, Any]) -> Any:
    req = VerifyRateRequest()
    if "templateId" in j: req.template_id = int(j["templateId"])
    if "knownAnswers" in j:
        for k, v in j["knownAnswers"].items():
            req.known_answers[int(k)] = v
    return req


# ---------------------------------------------------------------------------
# JSON 序列化：用 protobuf 的 MessageToJson / Parse
# ---------------------------------------------------------------------------

def _serialize_pb(msg) -> Dict[str, Any]:
    """protobuf message -> dict (via protobuf JSON 反射)."""
    from google.protobuf import json_format
    try:
        return json_format.MessageToDict(
            msg,
            preserving_proto_field_name=True,
            including_default_value_fields=False,
        )
    except TypeError:
        # 旧版 protobuf 不支持 including_default_value_fields
        return json_format.MessageToDict(
            msg,
            preserving_proto_field_name=True,
        )


def _make_request(j: Dict[str, Any], method: str):
    """根据 method 字符串构造对应的 protobuf request 对象."""
    if method == "ParseGoldenTemplate":
        return _make_golden_template_request(j)
    if method in ("RecognizeByTemplate", "ReverifyPaper"):
        return _make_recognize_request(j)
    if method == "VerifyRecognitionRate":
        return _make_verify_rate_request(j)
    raise ValueError(f"unknown method: {method}")


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

def _make_handler(servicer: OmrServiceServicer, health_path: str = "/health", crop_output_dir: Optional[str] = None):
    """Build a request handler class bound to the given servicer.

    用闭包而非实例属性,因为 BaseHTTPRequestHandler 由 HTTPServer 直接构造,
    不接受额外参数。
    """

    crop_root = Path(crop_output_dir).resolve() if crop_output_dir else None

    class _Handler(BaseHTTPRequestHandler):
        # 显式声明避免 IDE 警告
        server_version = "OMR-HTTP/1.0"

        def log_message(self, format, *args):  # noqa
            logger.debug(format, *args)

        def do_GET(self):
            if self.path == health_path:
                self._reply_json(200, {"status": "UP"})
            elif crop_root and self.path.startswith("/omr_crops/"):
                self._serve_crop()
            else:
                self._reply_json(404, {"error": "not found", "path": self.path})

        def _serve_crop(self):
            """提供主观题裁剪图片静态访问（带 CORS）。"""
            try:
                relative = self.path[len("/omr_crops/"):]
                # 禁止路径穿越
                target = (crop_root / relative).resolve()
                if crop_root not in target.parents and target != crop_root:
                    self._reply_json(403, {"error": "forbidden"})
                    return
                if not target.is_file():
                    self._reply_json(404, {"error": "file not found", "path": self.path})
                    return
                content = target.read_bytes()
                content_type, _ = mimetypes.guess_type(str(target))
                content_type = content_type or "application/octet-stream"
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Cache-Control", "public, max-age=3600")
                self.end_headers()
                self.wfile.write(content)
            except Exception as e:
                logger.exception("提供裁剪图片失败: %s", self.path)
                self._reply_json(500, {"error": "internal server error"})

        def do_POST(self):
            path = self.path.rstrip("/")
            mapping: Dict[str, Tuple[str, Callable[[Any], Any]]] = {
                "/parse_golden_template": ("ParseGoldenTemplate", servicer.ParseGoldenTemplate),
                "/recognize_by_template": ("RecognizeByTemplate", servicer.RecognizeByTemplate),
                "/reverify_paper":        ("ReverifyPaper",        servicer.ReverifyPaper),
                "/verify_recognition_rate": ("VerifyRecognitionRate", None),
            }
            entry = mapping.get(path)
            if not entry:
                self._reply_json(404, {"error": "not found", "path": self.path})
                return
            grpc_method, func = entry

            try:
                body = self._read_body_json()
                if not isinstance(body, dict):
                    self._reply_json(400, {"code": -1, "error": "request body must be a JSON object"})
                    return
            except Exception as e:
                self._reply_json(400, {"code": -1, "error": f"invalid json: {e}"})
                return

            if grpc_method == "VerifyRecognitionRate":
                # verify 还需要 mapping known_answers, 暂时未暴露
                self._reply_json(501, {"code": -1, "error": "VerifyRecognitionRate not exposed via HTTP yet"})
                return

            try:
                req = _make_request(body, grpc_method)
                logger.info("HTTP 调用 %s: %s", grpc_method, body)
                resp = func(req, context=None)
                payload = _serialize_pb(resp)
                # protobuf JSON 序列化默认跳过 code=0 等默认值字段，
                # 客户端（Java 端 callOmrParseGoldenTemplateViaHttp）依赖 code 字段，
                # 这里显式注入确保客户端能读到 code。
                if "code" not in payload:
                    payload["code"] = getattr(resp, "code", 0)
                self._reply_json(200, payload)
            except (ValueError, KeyError) as e:
                logger.warning("HTTP 调用 %s 请求参数无效: %s", grpc_method, e)
                self._reply_json(400, {"code": -1, "error": f"invalid request parameter: {e}"})
            except Exception as e:
                logger.exception("HTTP 调用 %s 失败", grpc_method)
                # 生产环境不对外暴露内部异常细节，同时防止响应写入失败拖垮 handler 线程
                try:
                    self._reply_json(500, {"code": -1, "error": "Internal server error"})
                except Exception:
                    pass

        # ---- helpers ----
        def _read_body_json(self) -> Dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            if not raw:
                return {}
            return json.loads(raw.decode("utf-8"))

        def _reply_json(self, status: int, payload: Dict[str, Any]):
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()
            try:
                self.wfile.write(data)
            except BrokenPipeError:
                # 客户端中途断开是常见场景,不影响服务端
                pass

        def do_OPTIONS(self):
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

    return _Handler


class OmrHttpServer:
    """OMR HTTP wrapper around Dubbo Tri servicer.

    与 dubbo_tri_server 并行运行;客户端可以绕过 Dubbo 直接调 HTTP。
    """

    def __init__(self, servicer: OmrServiceServicer, port: int, crop_output_dir: Optional[str] = None):
        self.port = port
        self._servicer = servicer
        self._crop_output_dir = crop_output_dir
        self._server: HTTPServer = None
        self._thread: threading.Thread = None

    def start(self):
        handler_cls = _make_handler(self._servicer, crop_output_dir=self._crop_output_dir)
        # ThreadingHTTPServer 让每个请求用独立线程处理
        from http.server import ThreadingHTTPServer
        self._server = ThreadingHTTPServer(("0.0.0.0", self.port), handler_cls)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info("OMR HTTP server started: :%s (serving Dubbo servicer over JSON)", self.port)

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            logger.info("OMR HTTP server stopped")