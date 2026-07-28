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