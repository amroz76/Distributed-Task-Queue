import os
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.models import JobAccepted, JobCreate, JobDetails
from app.storage import create_job, get_job


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Read connection address supplied by Docker Compose
    # Create client
    app.state.redis = Redis.from_url(
        os.environ["REDIS_URL"],

        # Return Redis strings as Python strings instead of bytes
        decode_responses=True,

        # Limit how long connecting and individual socket operations can wait.
        socket_connect_timeout=2,
        socket_timeout=2,
    )

    try:
        yield
    finally:
        # Release client's connections when API shuts down
        await app.state.redis.aclose()


app = FastAPI(lifespan=lifespan)


@app.exception_handler(RedisError)
async def redis_error_handler(
    request: Request,
    exc: RedisError,
):
    # Turn unhandled Redis errors from endpoints into a consistent response
    return JSONResponse(
        status_code=503,
        content={"detail": "Redis is unavailable"},
    )


@app.get("/health")
async def health(request: Request):
    # Send Redis command to check dependency connectivity
    await request.app.state.redis.ping()

    return {"status": "ok", "redis": "connected"}


@app.post(
    "/jobs",

    # Validate and serialize returned data
    response_model=JobAccepted,

    status_code=status.HTTP_202_ACCEPTED,
)
async def submit_job(body: JobCreate, request: Request):
    job_id = await create_job(request.app.state.redis, body)

    return JobAccepted(id=job_id, status="queued")


@app.get("/jobs/{job_id}", response_model=JobDetails)
async def read_job(job_id: UUID, request: Request):
    job = await get_job(request.app.state.redis, str(job_id))

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return job