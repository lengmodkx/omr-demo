from pydantic import BaseModel, Field

from omr_service.api.schemas.common import BaseResponse, Region, SubjectiveRegion
from omr_service.api.schemas.recognize import (
    PersonalInfo,
    QuestionAnswer,
    SubjectiveCrop,
)


class ColumnConfig(BaseModel):
    column_id: str
    column_index: int
    question_start: int
    question_count: int
    options_per_question: int = 4
    question_type: str = "single"


class GoldenTemplateRequest(BaseModel):
    template_id: str
    template_image_url: str
    columns: list[ColumnConfig] = Field(min_length=1)
    personal_info_region: Region | None = None
    subjective_regions: list[SubjectiveRegion] | None = None


class BubbleGrid(BaseModel):
    row: int
    col: int
    question_no: int
    option: str
    x: int
    y: int


class GoldenTemplateResponse(BaseResponse):
    template_id: str
    answers: list[QuestionAnswer] = Field(default_factory=list)
    bubble_grid: list[BubbleGrid] = Field(default_factory=list)
    personal_info_sample: PersonalInfo | None = None
    subjective_crops: list[SubjectiveCrop] | None = None
