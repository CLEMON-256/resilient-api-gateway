import time
import logging
import redis
from typing import Callable, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DistributedCircuitBreaker")

class CircuitBreakerOpenException(Exception):
    """Raised when the circuit is OPEN and fast-failing traffic."""
    pass

class RedisCircuitBreaker:
    def __init__(self, redis_host: str = "redis", redis_port: int = 6379, failure_threshold: int = 3, recovery_timeout: float = 10.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        
        # Connect to the Redis container using Docker DNS
        self.r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        
        # Redis Keys definitions
        self.STATE_KEY = "cb:state"
        self.FAILURE_KEY = "cb:failures"
        self.LAST_CHANGE_KEY = "cb:last_change"

        # Initialize defaults safely in Redis if they don't exist yet
        self.r.setnx(self.STATE_KEY, "CLOSED")
        self.r.setnx(self.FAILURE_KEY, 0)
        self.r.setnx(self.LAST_CHANGE_KEY, time.time())

    # Helper properties to fetch real-time state data straight out of Redis memory
    @property
    def state(self) -> str:
        return self.r.get(self.STATE_KEY)

    @property
    def failure_count(self) -> int:
        return int(self.r.get(self.FAILURE_KEY) or 0)

    @property
    def last_state_change(self) -> float:
        return float(self.r.get(self.LAST_CHANGE_KEY) or time.time())

    async def call(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        current_time = time.time()
        current_state = self.state

        # Check if an OPEN circuit is ready to transition to HALF-OPEN based on timestamps stored in Redis
        if current_state == "OPEN":
            if current_time - self.last_state_change >= self.recovery_timeout:
                self._change_state("HALF-OPEN")
                current_state = "HALF-OPEN"
            else:
                raise CircuitBreakerOpenException("Circuit is OPEN. Fast-failing traffic safely via Redis state.")

        try:
            result = await func(*args, **kwargs)
            
            # If a request succeeds in HALF-OPEN, close the loop completely
            if current_state == "HALF-OPEN":
                self._change_state("CLOSED")
                
            return result
        except Exception as e:
            self._handle_failure(e)
            raise e

    def _handle_failure(self, error: Exception):
        # Increment the shared failure counter in Redis atomically
        new_failures = self.r.incr(self.FAILURE_KEY)
        logger.warning(f"Failure detected globally ({new_failures}/{self.failure_threshold}): {error}")

        current_state = self.state
        if current_state in ("CLOSED", "HALF-OPEN") and new_failures >= self.failure_threshold:
            self._change_state("OPEN")

    def _change_state(self, new_state: str):
        logger.info(f"🚨 REDIS STATE TRANSITION: {self.state} ──> {new_state}")
        
        # Use a Redis pipeline transaction to set states and timestamps cleanly
        pipe = self.r.pipeline()
        pipe.set(self.STATE_KEY, new_state)
        pipe.set(self.LAST_CHANGE_KEY, time.time())
        if new_state == "CLOSED":
            pipe.set(self.FAILURE_KEY, 0)
        pipe.execute()
