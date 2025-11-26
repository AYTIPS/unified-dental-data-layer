from core.database import SessionLocal
from core.models import Patients, RegisteredClinics, Appointments
from api.routers import webhook_crm
from sdk.opendental_sdk import openDentalApi
from core.schemas import patient_model, Appointments_create, Webhook_requests
from core.utils import book_appointment
from core.circuti_breaker import circuit_breaker_open_error
import logging
from core.queue import appointments_queue
logger = logging.getLogger(__name__)

async def process_crm_load (clinic_id : str , crm_type: str , payload: dict ):
    db = SessionLocal()
    try:
        clinic = db.query(RegisteredClinics).filter_by(id = clinic_id).first()
        if not clinic:
             raise ValueError("clinic id  {clinic} not found ")
        
        timezone = clinic.clinic_timezone

        patient_data = patient_model(**payload)

        commslog = payload.get("commslog")

        date_str =  payload.get(" Date")

        start_str = payload.get("start_time")

        end_str = payload.get("end_time")

        status = payload.get("status") 

        calendar_id = payload.get("calendar_id")

        event_id = payload.get("event_id")

        contact_id = payload.get("contact_id")

        Note = payload.get("Note")

        pop_up = payload.get("pop_up")

        od = openDentalApi(clinic_id)

        existing_patient = db.query(Patients).filter_by(clinic_id = clinic_id , contact_id = contact_id).first()

        if existing_patient:
            pat_num = existing_patient.pat_num

        if not existing_patient:
            check = await od.search_patients(last_name = patient_data.LName , date_of_birth = patient_data.Birthdate)
            pat_num = check[0]["PatNum"] if not check else None
        else:
            created = await od.create_patients (patient_data = patient_data)
            pat_num = created["PatNum"]
        

        new_patient = Patients( 
                    FName = patient_data.FName,
                    LName = patient_data.LName,
                    Gender = patient_data.Gender,
                    phone = patient_data.WirelessPhone,
                    email =  patient_data.Email,
                    pat_num = pat_num,
                    contact_id = patient_data.contact_id,
                    clinic_id  = clinic_id,
                    )
        

        # create appointment for the patient 
        created_appointment = await book_appointment(od = od , clinic = clinic , date_str = date_str, start_str = start_str, end_str = end_str , status = status, calendar_id = calendar_id , pat_num = pat_num ,  clinic_timezone = timezone, clinic_id = clinic_id , event_id = event_id, Note = Note ,  commlogs = commslog , create_popup = pop_up )
        if  created_appointment is None:
            raise ValueError("book_appointment() returned None. Appointment could not be booked.")

        new_appointment = Appointments(
            clinic_id=clinic_id,
            event_id=event_id,
            start_time =  start_str,
            end_time =   end_str,
            date = date_str, 
            status=status,
            AptNum = created_appointment["AptNum"],
            calendar_id = calendar_id 
            )                   
        

        db.add(new_patient)
        db.add(new_appointment)
        db.commit()
        db.refresh(new_patient)
        db.refresh(new_appointment)
        return pat_num, created_appointment
    
    except circuit_breaker_open_error:
         appointments_queue.enqueue(process_crm_load, clinic_id = clinic_id , crm_type = crm_type, payload = payload )
         return
        

    except Exception as e :
            db.rollback()
            raise ValueError(f"Error processing patient : {e}")
            
        
    finally:
            db.close()


            





                