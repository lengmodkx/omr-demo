"""OMR 业务异常体系.

错误码编码 (与 omr.proto 兼容):
    0  成功
    4  模板未找到
    5  图片加载失败
    6  请求参数非法
    7  任务不存在
    99 内部错误
"""
from __future__ import annotations

from urllib.parse import urlparse


class OmrError(Exception):
    """所有 OMR 业务异常的基类."""

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
        # 去除 query string 避免日志泄露签名 token
        parsed = urlparse(url)
        safe_url = parsed._replace(query="").geturl() if parsed.scheme else url
        super().__init__(f"图片加载失败: {safe_url} ({reason})")


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