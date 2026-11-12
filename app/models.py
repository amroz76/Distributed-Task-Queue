from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class SumNumbersArgs(BaseModel):
    # Reject unexpected fields and special numbers such as infinity or NaN.
    model_config = ConfigDict(
        extra="forbid",
        allow_inf_nan=False,
    )

    # Require a nonempty list, with a limit to keep request sizes reasonable.
    # Pydantic can convert ordinary integers such as 1 into floats such as 1.0.
    numbers: list[float] = Field(min_length=1, max_length=1000)


class JobCreate(BaseModel):
    # Catch unexpected fields rather than silently ignoring them.
    model_config = ConfigDict(extra="forbid")

    # Only this task name is supported for now.
    task: Literal["sum_numbers"]

    # Validate the nested "args" object using the model above.
    args: SumNumbersArgs


class JobAccepted(BaseModel):
    # The response returned immediately after accepting a submission.
    id: str
    status: Literal["queued"]


class JobDetails(BaseModel):
    # The response returned when a client looks up an existing job.
    id: str
    task: str
    args: SumNumbersArgs
    status: str
    attempts: int
    created_at: str