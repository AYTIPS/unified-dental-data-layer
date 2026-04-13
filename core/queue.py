from redis import Redis
from redis.asyncio import Redis as AsyncRedis
from rq import Queue
from config import settings



redis_client = Redis.from_url(
    settings.redis_url,
    decode_responses=True,
)
async_redis = AsyncRedis.from_url(
    settings.redis_url,
    decode_responses=True,
)

# create a queue
appointments_queue = Queue("appointments", connection=redis_client)




