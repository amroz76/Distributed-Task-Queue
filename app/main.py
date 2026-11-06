import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from redis.asyncio import Redis
from redis.exceptions import RedisError

#Creates Redis client when API starts, closes it when API stops
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = Redis.from_url(
        os.environ["REDIS_URL"],
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
    )
    try:
        yield
    finally:
        await app.state.redis.aclose()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health(request: Request):
    try:
        await request.app.state.redis.ping() #Ping Redis
    except RedisError:
        raise HTTPException(
            status_code=503, #Return 503 error if service is unavailable
            detail="Redis is unavailable",
        ) from None

    return {"status": "ok", "redis": "connected"} #Return Redis status