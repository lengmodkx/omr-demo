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


def test_question_answer_fields():
    """QuestionAnswer 新结构：question_no/answer/status/correct"""
    a = QuestionAnswer(question_no=1, answer="A")
    assert a.answer == "A"
    assert a.status == "empty"
    assert a.correct is None


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
