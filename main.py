from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from core import database
from fastapi.middleware.cors import CORSMiddleware
import logging 

log = logging.getLogger("uvicorn.error")

origins = ["*"]
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(webhook.router)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc:RequestValidationError):
    path = request.url.path
    if  not path.startswith("/webhook/"):
        return JSONResponse(
            status_code=422,
            content = {"detail":exc.errors()}
        )
    clinic_id = request.path_params.get("clinic_id")
    crm_type = request.path_params.get("crm_type")
    try:
        payload = await request.json()
    except:
        payload = {}

    errors = exc.errors()






@app.on_event("startup")
def verify_db_on_start():
    ok, msg = database.ping_db()
    if ok:
        log.info(msg)              # you'll see this in your console
    else:
        log.error(msg)




app.get('/')
async def root():
     return {"status": "running", "message": "Welcome to OpenDental CRM Sync API"}
    