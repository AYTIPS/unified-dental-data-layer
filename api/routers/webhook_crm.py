from fastapi import  APIRouter, Request, HTTPException, status, Depends
from sqlalchemy.orm import Session
from core.queue import redis_client
from core.database import get_db
from core.queue import appointments_queue
from core.models import RegisteredClinics
from core.schemas import Webhook_requests, webhook_response
from workers.workers import process_crm_load

router= APIRouter( 
    prefix= "/webhook",
    tags= ["CRM Webhooks"]
    )
  
    

@router.post("/webhook/{crm_type}/{clinic_id}", status_code=202, response_model = webhook_response)
async def webhooks(crm_type: str, clinic_id: str, payload: Webhook_requests , db: Session = Depends(get_db)):
     # check if clinic is there 
    clinic = db.query(RegisteredClinics).filter_by(id=clinic_id).first()
    if not clinic:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail = "clinic not found wwrong webhook url ")

    # checks if the crm supported is the one returned 
    if not clinic.crm_type.lower() != crm_type.lower():
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail= "incorrect webhook url")
    #extract payload for redis use 
    payload_dict = payload.model_dump()
    event_id = payload_dict.get("event_id")
    contact_id = payload_dict.get("contact_id")

    #idempotency to avoid duplicate 
    redis_key = f"webhook processing: {event_id}:{contact_id}"
    if redis_client.exists(redis_key):
        raise ValueError(f"duplicate webhook processing :{event_id}:{contact_id}")
    else:
        redis_client.setex(redis_key, 300, "processing")


    #queue the job 
    job = appointments_queue.enqueue(process_crm_load, payload.model_dump(), crm_type = crm_type, clinic_id = clinic_id)

    return {"status" : status.HTTP_200_OK ,
            "payload": payload,
            "job_id" : job.id,
            "message": "Webhook processed successfully",
            "clinic": clinic_id,
            "crm_type": crm_type}



        
        


    

    