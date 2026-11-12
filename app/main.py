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
    # Read the connection address supplied by Docker Compose.
    # Create one client for this API process, rather than one per request.
    app.state.redis = Redis.from_url(
        os.environ["REDIS_URL"],

        # Return Redis strings as Python strings instead of bytes.
        decode_responses=True,

        # Limit how long connecting and individual socket operations can wait.
        socket_connect_timeout=2,
        socket_timeout=2,
    )

    try:
        # FastAPI serves requests while execution is paused at this yield.
        yield
    finally:
        # Release the client's connections when the API shuts down.
        await app.state.redis.aclose()


# Register our startup/shutdown lifecycle with FastAPI.
app = FastAPI(lifespan=lifespan)


@app.exception_handler(RedisError)
async def redis_error_handler(
    request: Request,
    exc: RedisError,
):
    # Turn unhandled Redis errors from endpoints into a consistent response.
    # The parameters are required by the exception-handler interface.
    return JSONResponse(
        status_code=503,
        content={"detail": "Redis is unavailable"},
    )


@app.get("/health")
async def health(request: Request):
    # Send an actual Redis command to check dependency connectivity.
    await request.app.state.redis.ping()

    return {"status": "ok", "redis": "connected"}


@app.post(
    "/jobs",

    # Validate and serialize our returned data using this response model.
    response_model=JobAccepted,

    # Accepted for later processing; execution hasn't happened yet.
    status_code=status.HTTP_202_ACCEPTED,
)
async def submit_job(body: JobCreate, request: Request):
    # FastAPI validates the JSON body before calling this function.
    # Request gives us access to the application's shared Redis client.
    job_id = await create_job(request.app.state.redis, body)

    return JobAccepted(id=job_id, status="queued")


@app.get("/jobs/{job_id}", response_model=JobDetails)
async def read_job(job_id: UUID, request: Request):
    # FastAPI validates the URL parameter as a UUID.
    # Convert it to its standard string form to construct the Redis key.
    job = await get_job(request.app.state.redis, str(job_id))

    if job is None:
        # A validly formatted ID can still refer to a nonexistent job.
        raise HTTPException(status_code=404, detail="Job not found")

    return job