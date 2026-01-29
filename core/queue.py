from redis import Redis
from redis.asyncio import Redis as AsyncRedis
from rq import Queue

# connect to redis
redis_client = Redis(host="localhost", port=6379, db=0, decode_responses=True)
async_redis = AsyncRedis(host="localhost", port=6379, db=0, decode_responses=True)

# create a queue
appointments_queue = Queue("appointments", connection=redis_client)




