from fastapi import APIRouter, Depends,Response, Cookie, Header, Request
from auth.csrf_helper import verify_csrf
from core.database import get_db
from core.models import Users
from sqlalchemy.orm import Session
from core.schemas import logoutresponse
from auth.oauth2 import get_current_user, clear_stream_access_cookie


router = APIRouter(
    prefix= "/logout",
    tags= ["Auth"]
)

@router.post("", status_code= 200)
async def logout(
    response: Response,
    current_user: Users = Depends(get_current_user),
    db: Session = Depends(get_db),
  ):
 
    current_user.token_version = int(current_user.token_version + 1)
    current_user.refresh_jti = None
    db.commit()

    response.delete_cookie(key="refresh_token", path="/login/refresh")
    response.delete_cookie(key="csrf_token", path="/") 
    clear_stream_access_cookie(response)

    return logoutresponse(message="You Have been Logged Out Successfully")
