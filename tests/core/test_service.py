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
    img = np.zeros((100, 100, 3), dtype=np.uint8)
    mock_deps["image_loader"].load.return_value = [img]
    mock_deps["template_store"].get.return_value = MagicMock()
    mock_deps["worker_pool"].submit.return_value.result.return_value = (
        [
            {"question_no": 1, "selected": ["A"], "is_blank": False, "is_multiple": False, "answer_type": "single"}
        ],
        False,
    )

    result = service.recognize({
        "template_id": "t-1",
        "scan_image_urls": ["http://x/y.jpg"],
    })

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
            "columns": [],
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


def test_recognize_includes_ocr_personal_info(service, mock_deps):
    """OCR 识别个人信息 (mock 返回)."""
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
