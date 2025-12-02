from redis import Redis
from rq import Queue

#connect to redis 
redis_client = Redis(host='localhost', port = 6379, db= 0, decode_responses= True)


#creat a queue
appointments_queue = Queue("appointments", connection=redis_client)




