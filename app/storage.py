import json
from datetime import datetime, timezone
from uuid import uuid4

from redis.asyncio import Redis

from app.models import JobCreate, JobDetails


# All work messages go into this Redis stream.
STREAM_KEY = "jobs"


async def create_job(redis: Redis, request: JobCreate) -> str:
    # Generate an application job ID independently of Redis's stream entry ID.
    job_id = str(uuid4())

    # Redis hash fields hold strings or bytes.
    # Serialize the nested arguments into JSON text before storing them.
    job_data = {
        "id": job_id,
        "task": request.task,
        "args": request.args.model_dump_json(),
        "status": "queued",
        "attempts": "0",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    # Queue both commands locally, then submit them as a Redis transaction.
    # Other clients cannot run commands between these two writes.
    async with redis.pipeline(transaction=True) as transaction:
        # Store the complete job record under a unique key.
        transaction.hset(f"job:{job_id}", mapping=job_data)

        # Add a work ticket pointing to that record.
        # Redis generates and returns a separate stream entry ID.
        transaction.xadd(STREAM_KEY, {"job_id": job_id})

        # This is where the queued commands are sent and executed.
        await transaction.execute()

    return job_id


async def get_job(redis: Redis, job_id: str) -> JobDetails | None:
    # Fetch every field from this job's Redis hash.
    job_data = await redis.hgetall(f"job:{job_id}")

    # HGETALL returns an empty dictionary when the key doesn't exist.
    if not job_data:
        return None

    # Convert Redis's stored strings back into the API's structured data.
    return JobDetails(
        id=job_data["id"],
        task=job_data["task"],
        args=json.loads(job_data["args"]),
        status=job_data["status"],
        attempts=int(job_data["attempts"]),
        created_at=job_data["created_at"],
    )