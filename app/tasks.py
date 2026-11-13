from collections.abc import Callable
from typing import Any

from app.models import SumNumbersArgs


def sum_numbers(args: dict[str, Any]) -> float:
    # Validate stored arguments before executing the task.
    # API validation protects submission; this also protects the worker
    # if malformed data reaches Redis through another route.
    validated_args = SumNumbersArgs.model_validate(args)

    return sum(validated_args.numbers)


# Only functions registered here can be executed by a worker.
# Never evaluate client input as Python code or dynamically import a
# client-supplied function path.
TASK_REGISTRY: dict[str, Callable[[dict[str, Any]], Any]] = {
    "sum_numbers": sum_numbers,
}