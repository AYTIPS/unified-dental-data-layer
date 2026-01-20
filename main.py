from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from core import database
from  api.routers import webhook_crm
from core.middleware import RateLimitMiddleware
from fastapi.middleware.cors import CORSMiddleware
import logging 

log = logging.getLogger("uvicorn.error")
logging.basicConfig(
    level=logging.INFO,  # or DEBUG
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

origins = ["*"]
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],

)
app.add_middleware(RateLimitMiddleware)
app.include_router(webhook_crm.router)


@app.on_event("startup")
def verify_db_on_start():
    ok, msg = database.ping_db()
    if ok:
        log.info(msg)             
    else:
        log.error(msg)

app.get('/')
async def root():
     return {"status": "running", "message": "Welcome to OpenDental CRM Sync API"}
    