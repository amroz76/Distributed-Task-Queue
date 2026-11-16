from collections.abc import Callable
from typing import Any

from app.models import SumNumbersArgs


#Task for testing purposes. Just sums numbers provided in argument
def sum_numbers(args: dict[str, Any]) -> float:
    validated_args = SumNumbersArgs.model_validate(args)

    return sum(validated_args.numbers)


TASK_REGISTRY: dict[str, Callable[[dict[str, Any]], Any]] = {
    "sum_numbers": sum_numbers,
}