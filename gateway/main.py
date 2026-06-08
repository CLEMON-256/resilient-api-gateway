from fastapi import FastAPI, Response, status, Request
import httpx
import itertools
import time
from contextlib import asynccontextmanager
from circuit_breaker import RedisCircuitBreaker, CircuitBreakerOpenException

video_service_breaker = RedisCircuitBreaker(redis_host="redis", redis_port=6379, failure_threshold=3, recovery_timeout=10.0)

VIDEO_SERVICES = [
    "http://video_service_1:8001/api/v1/videos",
    "http://video_service_2:8002/api/v1/videos"
]
service_pool = itertools.cycle(VIDEO_SERVICES)

# 🛠️ Define the Redis Lua Script for the Sliding Window Rate Limiter
# This runs atomically inside Redis. It removes old requests and checks the current count.
LUA_RATE_LIMITER = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

local clear_before = now - window

-- Remove elements older than the current window frame
redis.call('ZREMRANGEBYSCORE', key, 0, clear_before)

-- Count how many requests this IP has made within the window
local current_requests = redis.call('ZCARD', key)

if current_requests < limit then
    -- Add the current timestamp as both the score and the member value
    redis.call('ZADD', key, now, now)
    -- Set an explicit TTL on the key so it cleans itself up if the user stops clicking
    redis.call('EXPIRE', key, window)
    return 1
else
    return 0
end
"""

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(0.2, connect=0.1)
    )
    yield
    await app.state.http_client.aclose()

app = FastAPI(title="Netflix-Style Resilient Gateway", lifespan=lifespan)

# 🚀 Senior Middleware: Intercept requests to enforce the Centralized Rate Limiter
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # Only protect our critical telemetry/data endpoints
    if request.url.path == "/trending":
        # Identify the user by their network IP address
        client_ip = request.client.host
        redis_key = f"rate_limit:{client_ip}"
        
        # Configuration: Max 5 requests every 10 seconds per IP
        now = time.time()
        window_seconds = 10
        max_limit = 5
        
        # Register and execute the Lua script directly on the Redis client instance
        redis_client = video_service_breaker.r
        lua_script = redis_client.register_script(LUA_RATE_LIMITER)
        
        # Execute the script atomically
        is_allowed = lua_script(keys=[redis_key], args=[now, window_seconds, max_limit])
        
        if not is_allowed:
            # Block malicious traffic instantly right at the front door with a 429 status code
            return Response(
                content='{"error": "Too Many Requests. Rate limit exceeded. Relax!"}',
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                media_type="application/json"
            )
            
    return await call_next(request)

@app.get("/")
async def read_root():
    return {
        "gateway_status": "online", 
        "security_layer": "Atomic Sliding Window Rate Limiter (Active)",
        "cluster_nodes": VIDEO_SERVICES
    }

async def fetch_from_node(client: httpx.AsyncClient, url: str):
    response = await client.get(url)
    response.raise_for_status()
    return {"responding_node": url, "payload": response.json()}

@app.get("/trending")
async def get_trending_videos():
    target_node_url = next(service_pool)
    client = app.state.http_client
    try:
        data = await video_service_breaker.call(fetch_from_node, client, target_node_url)
        return {"source": "live_cluster", "data": data}
    except CircuitBreakerOpenException:
        return get_fallback_payload("Circuit is OPEN. Fast-failing traffic safely via Redis.")
    except (httpx.HTTPError, httpx.TimeoutException):
        return get_fallback_payload(f"Node {target_node_url} timed out or failed. Cache served.")

def get_fallback_payload(reason: str):
    return {
        "source": "gateway_fallback_cache",
        "status": "degraded_resilience_active",
        "reason": reason,
        "data": [
            {"id": 101, "title": "Stranger Things (Cached)"}
        ]
    }
