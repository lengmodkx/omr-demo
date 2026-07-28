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
