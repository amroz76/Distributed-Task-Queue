import asyncio
import json
import logging
import os
import socket

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.storage import STREAM_KEY, utc_now
from app.tasks import TASK_REGISTRY


# Every worker uses the same group so Redis distributes messages among them.
# Different groups would independently receive the stream's messages.
GROUP_NAME = "workers"

logger = logging.getLogger(__name__)


async def ensure_consumer_group(redis: Redis) -> None:
    try:
        await redis.xgroup_create(
            name=STREAM_KEY,
            groupname=GROUP_NAME,

            # Start at the beginning so jobs submitted before the first
            # worker starts are also eligible for delivery.
            id="0-0",

            # Create an empty stream if no jobs have been submitted yet.
            mkstream=True,
        )
    except ResponseError as exc:
        # Multiple workers may attempt to create the group.
        # An existing group is expected; other Redis errors are not.
        if not str(exc).startswith("BUSYGROUP"):
            raise


async def process_message(
    redis: Redis,
    worker_id: str,
    message_id: str,
    fields: dict[str, str],
) -> None:
    # The stream holds a reference; the hash holds the complete job.
    job_id = fields.get("job_id")

    if job_id is None:
        # This malformed message cannot be executed. Log and acknowledge
        # it so it doesn't remain pending forever.
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
        # Avoid repeating a job whose terminal state is already recorded.
        # This is a basic guard, not a complete exactly-once guarantee.
        await redis.xack(STREAM_KEY, GROUP_NAME, message_id)
        return

    # Record that execution is starting and increment the attempt counter.
    # HINCRBY performs the increment inside Redis.
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

        # These first tasks are ordinary synchronous functions.
        # Each worker intentionally executes one job at a time.
        result = task_function(task_args)

        # Serialize before recording success. Reject non-finite results,
        # which aren't valid standard JSON, even if inputs were finite.
        result_json = json.dumps(result, allow_nan=False)

    except Exception as exc:
        # Catch task/argument/serialization failures so one bad job doesn't
        # terminate the worker. Redis operations remain outside this block:
        # a storage failure must not be mistaken for a task failure.
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

            # A failed job is terminal in this first implementation.
            # We will change this behavior when we introduce retries.
            transaction.xack(STREAM_KEY, GROUP_NAME, message_id)
            await transaction.execute()

        return

    # Record the result and acknowledge the message in one transaction.
    # Acknowledgment removes it from the group's pending tracking;
    # it does not delete the entry from the stream.
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
    # Docker gives each container its own hostname. Allow an explicit
    # override too. Consumer names must be distinct among running workers.
    worker_id = os.environ.get("WORKER_ID") or socket.gethostname()

    redis = Redis.from_url(
        os.environ["REDIS_URL"],
        decode_responses=True,
        socket_connect_timeout=2,

        # Longer than the five-second blocking read below.
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
                # It does not retrieve messages left pending by dead workers.
                streams={STREAM_KEY: ">"},

                # Claim one job at a time to avoid reserving a backlog
                # that this worker cannot immediately execute.
                count=1,

                # Wait up to five seconds rather than repeatedly polling.
                # Redis returns early as soon as a message is available.
                block=5000,
            )

            # The response is grouped by stream, even though we read only one.
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

    # Start the event loop that runs our asynchronous Redis operations.
    asyncio.run(main())