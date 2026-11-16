from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class SumNumbersArgs(BaseModel):
    # Reject unexpected fields and special numbers such as infinity or NaN
    model_config = ConfigDict(
        extra="forbid",
        allow_inf_nan=False,
    )

    # Require a nonempty list, with a limit to keep request sizes reasonable
    numbers: list[float] = Field(min_length=1, max_length=1000)


class JobCreate(BaseModel):
    # Catch unexpected fields
    model_config = ConfigDict(extra="forbid")

    task: Literal["sum_numbers"]

    # Validate the nested "args" object
    args: SumNumbersArgs


class JobAccepted(BaseModel):
    id: str
    status: Literal["queued"]


class JobDetails(BaseModel):
    id: str
    task: str
    args: SumNumbersArgs
    status: Literal["queued", "running", "succeeded", "failed"]
    attempts: int
    created_at: str

    started_at: str | None = None
    finished_at: str | None = None
    worker_id: str | None = None
    result: float | None = None
    error: str | None = None