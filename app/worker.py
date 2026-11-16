import asyncio
import json
import logging
import os
import socket

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.storage import STREAM_KEY, utc_now
from app.tasks import TASK_REGISTRY


# Every worker uses the same group so Redis distributes messages among them
GROUP_NAME = "workers"

logger = logging.getLogger(__name__)


async def ensure_consumer_group(redis: Redis) -> None:
    try:
        await redis.xgroup_create(
            name=STREAM_KEY,
            groupname=GROUP_NAME,

            # Start at beginning so jobs submitted before first
            # worker starts are eligible for delivery
            id="0-0",

            # Create an empty stream if no jobs have been submitted yet
            mkstream=True,
        )
    except ResponseError as exc:
        # Multiple workers may attempt to create the group
        # An existing group is expected; other Redis errors are not
        if not str(exc).startswith("BUSYGROUP"):
            raise


async def process_message(
    redis: Redis,
    worker_id: str,
    message_id: str,
    fields: dict[str, str],
) -> None:
    job_id = fields.get("job_id")

    if job_id is None:
        # Log and acknowledge malformed message
        logger.error("Message %s has no job_id", message_id)
        await redis.xack(STREAM_KEY, GROUP_NAME, message_id)
        return

    job_key = f"job:{job_id}"
    job_data = await redis.hgetall(job_key)

    if not job_data:
        logger.error("Message %s references missing job %s", message_id, job_id)
        await redis.xack(STREAM_KEY, GROUP_NAME, message_id)
        return

    if job_data["status"] in {"succeeded", "failed"}:
        # Avoid repeating a job whose terminal state is already recorded
        await redis.xack(STREAM_KEY, GROUP_NAME, message_id)
        return

    # Record that execution is starting and increment the attempt counter
    async with redis.pipeline(transaction=True) as transaction:
        transaction.hset(
            job_key,
            mapping={
                "status": "running",
                "worker_id": worker_id,
                "started_at": utc_now(),
            },
        )
        transaction.hincrby(job_key, "attempts", 1)
        await transaction.execute()

    logger.info("Starting job %s on worker %s", job_id, worker_id)

    try:
        task_name = job_data["task"]
        task_function = TASK_REGISTRY[task_name]
        task_args = json.loads(job_data["args"])

        result = task_function(task_args)

        # Serialize before recording success. Reject non-finite results,
        # which aren't valid standard JSON, even if inputs were finite
        result_json = json.dumps(result, allow_nan=False)

    except Exception as exc:
        # Catch task/argument/serialization failures so one bad job doesn't
        # terminate the worker.
        logger.exception("Job %s failed", job_id)

        async with redis.pipeline(transaction=True) as transaction:
            transaction.hset(
                job_key,
                mapping={
                    "status": "failed",
                    "finished_at": utc_now(),
                    "error": f"{type(exc).__name__}: {exc}",
                },
            )

            # A failed job is terminal in the current implementation.
            # We will change this behavior when we introduce retries
            transaction.xack(STREAM_KEY, GROUP_NAME, message_id)
            await transaction.execute()

        return

    # Record the result and acknowledge the message
    async with redis.pipeline(transaction=True) as transaction:
        transaction.hset(
            job_key,
            mapping={
                "status": "succeeded",
                "finished_at": utc_now(),
                "result": result_json,
            },
        )
        transaction.xack(STREAM_KEY, GROUP_NAME, message_id)
        await transaction.execute()

    logger.info("Job %s succeeded with result %s", job_id, result_json)


async def main() -> None:
    worker_id = os.environ.get("WORKER_ID") or socket.gethostname()

    redis = Redis.from_url(
        os.environ["REDIS_URL"],
        decode_responses=True,
        socket_connect_timeout=2,

        socket_timeout=10,
    )

    try:
        await ensure_consumer_group(redis)
        logger.info("Worker %s is waiting for jobs", worker_id)

        while True:
            messages = await redis.xreadgroup(
                groupname=GROUP_NAME,
                consumername=worker_id,

                # ">" requests messages not yet delivered to this group.
                streams={STREAM_KEY: ">"},

                # Claim one job at a time to avoid reserving a backlog
                # that this worker cannot immediately execute
                count=1,

                # Wait up to five seconds rather than repeatedly polling.
                # Redis returns early as soon as a message is available
                block=5000,
            )

            for stream_name, entries in messages:
                for message_id, fields in entries:
                    await process_message(
                        redis,
                        worker_id,
                        message_id,
                        fields,
                    )
    finally:
        await redis.aclose()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    asyncio.run(main())