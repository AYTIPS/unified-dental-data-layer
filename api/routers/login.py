from fastapi import Depends, APIRouter, status, HTTPException, Request
from core.queue import redis_client
from  core.database import get_db
from sqlalchemy.orm import Session
from core.models import Users
from auth.oauth2 import get_current_user
from core.schemas import loginresponse, loginrequest
from auth.oauth2 import create_access_token, create_refresh_token , verify_password
from infra.login_helper import handle_failed_login, login_attempts, get_redis_attempts , MAX_LOGIN_ATTEMPTS, get_client_ip
import logging

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix = "/login",
    tags = ["Auth"] 
)


@router.post("/", status_code=  200,  response_model= loginresponse)
async def login(payload: loginrequest, request: Request, db:Session= Depends(get_db)):
    email = payload.email
    password = payload.password
    ip = get_client_ip(request)
    key = await login_attempts(email, ip)

    attempts = await get_redis_attempts(key)
    if attempts >= MAX_LOGIN_ATTEMPTS:
        raise HTTPException( status.HTTP_429_TOO_MANY_REQUESTS, detail = "Too many login attempt please Try again Later")
    
    user = db.query(Users).filter(Users.email == email).first()
    if not user or not verify_password(password, hashed_password = user.password):
       await handle_failed_login(key)
    
    redis_client.delete(key)

    access_token = create_access_token(user = user)
    refresh_token = create_refresh_token(user = user)

    return {
        "access_token" : access_token,
        "refresh_token" : refresh_token
    }