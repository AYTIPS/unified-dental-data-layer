from core.queue import redis_client
from fastapi import HTTPException, status, Request

MAX_LOGIN_ATTEMPTS = 5 
LOCK_WINDOWS_SECONDS = 30 * 60 

async def login_attempts(email : str , ip: str ):
    return f"login attempts : {email} : {ip}"

async def get_redis_attempts(key) -> int:
    attempts_raw = await redis_client.get(key)
    return int(attempts_raw.decode()) if attempts_raw is not None else 0

async def increment_attempts_with_key(key):
    attempts =  await redis_client.incr(key)
    if attempts >= MAX_LOGIN_ATTEMPTS:
        redis_client.expire(key, LOCK_WINDOWS_SECONDS)
    return int(attempts)


async def  handle_failed_login(key):
    new_attempt = await increment_attempts_with_key(key)
    if new_attempt >= MAX_LOGIN_ATTEMPTS:
        raise HTTPException( status.HTTP_429_TOO_MANY_REQUESTS, detail= "Too many login attempt please Try again Later")
    raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail = "Invalid Email or Password ")

def get_client_ip(request: Request):
    X_forwarded_for = request.headers.get("X_forwarded_for")
    if X_forwarded_for:
        return X_forwarded_for.split(",")[0].strip()
    if request.client is not None:
        return request.client.host
    return "unknown"
    






