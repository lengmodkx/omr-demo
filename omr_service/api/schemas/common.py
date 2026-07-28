from pydantic import BaseModel, Field


class Region(BaseModel):
    x: int
    y: int
    width: int
    height: int


class SubjectiveRegion(Region):
    region_id: str


class ErrorResponse(BaseModel):
    code: int
    message: str
    request_id: str | None = None


class BaseResponse(BaseModel):
    code: int = 0
    message: str = "ok"
    elapsed_ms: int | None = None
