from fastapi import APIRouter, Depends
from core.database import get_db
from core.models import Users
from sqlalchemy.orm import Session
from core.schemas import logoutresponse
from auth.oauth2 import get_current_user



router = APIRouter(
    prefix= "/logout",
    tags= ["Auth"]
)

@router.post("/", status_code= 200)
async def logout(current_user : Users = Depends(get_current_user), db:Session= Depends(get_db)):
    current_user.token_version = int(current_user.token + 1)
    db.commit 
    return logoutresponse(
        message = "You Have been Logged Out Successfully"
    )
