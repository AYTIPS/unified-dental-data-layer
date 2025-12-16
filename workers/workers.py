from core.database import SessionLocal
from core.models import Patients, RegisteredClinics, Appointments
from api.routers import webhook_crm
from sdk.opendental_sdk import openDentalApi
from core.schemas import patient_model
from core.utils import book_appointment
from core.circuti_breaker import circuit_breaker_open_error
import logging
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
        else:
            check = await od.search_patients(last_name=patient_data.LName, date_of_birth=patient_data.Birthdate)
            if check:
                pat_num = check[0]["PatNum"]
            else:
                created = await od.create_patients(patient_data=patient_data)
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
         logger.warning(" Too many Failures Circuit breaker is still open", clinic_id,  crm_type,
            payload.get("event_id"),
            payload.get("contact_id"))
         raise
        

    except Exception as e :
            db.rollback()
            logger.exception(f"Error processing patient : {e}", clinic_id,  crm_type,
            payload.get("event_id"),
            payload.get("contact_id"))
            
        
    finally:
            db.close()


            





                