from pydantic import BaseModel, Field

from omr_service.api.schemas.common import BaseResponse, SubjectiveRegion
from omr_service.api.schemas.enums import AnswerType


class QuestionAnswer(BaseModel):
    question_no: int
    answer_type: AnswerType = AnswerType.SINGLE
    selected: list[str] = Field(default_factory=list)
    is_blank: bool = False
    is_multiple: bool = False
    confidence: float | None = None


class RecognizeRequest(BaseModel):
    template_id: str
    scan_image_urls: list[str] = Field(min_length=1)
    question_no: int | None = None
    personal_info_region: dict | None = None
    subjective_regions: list[SubjectiveRegion] | None = None


class PersonalInfo(BaseModel):
    name: str | None = None
    exam_id: str | None = None
    raw_text: str | None = None


class SubjectiveCrop(BaseModel):
    region_id: str
    url: str
    width: int
    height: int


class RecognizeResponse(BaseResponse):
    template_id: str
    answers: list[QuestionAnswer] = Field(default_factory=list)
    empty_count: int = 0
    multiple_count: int = 0
    abnormal: bool = False
    personal_info: PersonalInfo | None = None
    subjective_crops: list[SubjectiveCrop] | None = None
