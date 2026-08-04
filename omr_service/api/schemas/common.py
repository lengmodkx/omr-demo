from pydantic import BaseModel, ConfigDict, Field


class Region(BaseModel):
    x: int
    y: int
    width: int
    height: int


class SubjectiveRegion(BaseModel):
    """主观题区域（与 Java 端 OmrPayloadBuilder.buildSubjectiveRegions 对齐）.

    Java 发送 camelCase (pageIndex/stitchWithNext)，服务内部统一 snake_case。
    """

    model_config = ConfigDict(populate_by_name=True)

    q: int = 0
    x1: int = 0
    y1: int = 0
    x2: int = 0
    y2: int = 0
    page_index: int = Field(default=0, alias="pageIndex")
    stitch_with_next: bool = Field(default=False, alias="stitchWithNext")


class ErrorResponse(BaseModel):
    code: int
    message: str
    request_id: str | None = None


class BaseResponse(BaseModel):
    code: int = 0
    message: str = "ok"
    elapsed_ms: int | None = None
