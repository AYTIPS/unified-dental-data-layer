from core.schemas import patient_model, Appointments_create, Appointments_update, create_commslogs,  create_pop_ups, create_contact_ghl, create_appointment_ghl, update_appointment_ghl
from core.database import SessionLocal
from dateutil import parser
from core.models import Appointments
import pytz
import logging
import asyncio
import httpx
from datetime import datetime, timedelta

 
fmt = "%Y-%m-%d %H:%M:%S "
logger = logging.getLogger(__name__)

async def retry_with_bak_off ( func, retries: int = 5, base_delay : int = 1 , retry_on : tuple = (httpx.HTTPStatusError, httpx.RequestError)):
    delay = base_delay
    for attempt in range(retries):
        try:
            return await func()
        except retry_on as e:
            print( f"retry {attempt+1} failed due to {e}. Waiting {delay} before next try")
            await asyncio.sleep(delay)
            delay *= 2

    raise ValueError("Failed after max retries")


async def patient_payload(patient: patient_model  ):
    return{
        "LName": patient. LName,
        "FName":  patient.FName,
        "Gender": patient.Gender,
        "Birthdate" : patient.Birthdate,
        "Address": patient.Address,
       "WirelessPhone": patient.WirelessPhone,
       "Email": patient.Email
    }

async def appointment_payload (appointment :  Appointments_create):
    return{
      "PatNum":appointment.PatNum,
      "AptDateTime": appointment.AptDateTime,
      "Pattern" : appointment.Pattern,
      "Op" : appointment.Op,
      "AptStatus" :appointment.AptStatus,
      "Note" : appointment.Note
}

async def appointment_payload_update (appointment :  Appointments_update):
    return{
      "AptDateTime": appointment.AptDateTime,
      "Pattern" : appointment.Pattern,
      "Op" : appointment.Op,
      "AptStatus" :appointment.AptStatus,
      "Note" : appointment.Note
}

async def create_commlog(commlogs : create_commslogs):
    return{
        "PatNum" : commlogs.PatNum,
        "commlogs": commlogs.commlogs 
          }

async def create_pops(pop_up : create_pop_ups):
    return {
        "PatNum": pop_up.PatNum,
        "Description" : pop_up.pop_ups    }

async def opendental_pattern_time_build(date_str, start_time, end_time, clinic_timezone):
    #comibinig date and time 
    start_raw = f"{date_str}:{start_time}"
    end_raw = f"{date_str}:{end_time}"

    start_time = parser.parse(start_raw)
    end_time = parser.parse(end_raw)

    #localize 
    tz = pytz.timezone(clinic_timezone)
    start_time = tz.localize(start_time)
    end_time = tz.localize(end_time)    

    DateTimeStart = start_time.strftime(fmt)
    DateTimeEnd = end_time.strftime(fmt)

    #calculate time difference 
    diff = int((end_time-start_time).total_seconds() /60 )

    pattern = "X" * diff 

    return DateTimeStart, DateTimeEnd , pattern


async def  opendental_get_operatory_status (clinic , status, calendar_id ):
    mapping = clinic.operatory_calendar_map 
    status_list = mapping.get(status, [])
    matches = []

    for item in status_list:
        if item.get("calendar_id") == calendar_id:
            matches.append(item.get("operatories"))


def get_pattern_from_od(start_time_str:str, pattern:str):
    starttime =datetime.strptime( start_time_str, fmt) 
    duration_minutes = len(pattern) * 5
    endtime = starttime + timedelta(minutes = duration_minutes)
    return endtime    


async def check_time_slot(existing_appt, new_start_time , new_end_time ):
    for appt in existing_appt:
        starttime = datetime.strptime(appt["AptDateTime"], fmt)
        endtime = get_pattern_from_od(start_time_str= appt["AptDateTime"], pattern = appt["Pattern"])

        if (new_start_time < endtime) and (new_end_time > starttime):
            return False 
    return True 
 

async def book_appointment (od, clinic, date_str, start_str, end_str ,status, calendar_id, pat_num, clinic_timezone, clinic_id, event_id , Note , commlogs: str | None = None, create_popup: str | None = None ):
     db = SessionLocal()
     Aptnum_check = db.query(Appointments).filter_by(clinic_id = clinic_id , event_id = event_id).first()
     AptNum =  Aptnum_check.AptNum if Aptnum_check else None 
     operatories =  await opendental_get_operatory_status(clinic, status, calendar_id ) or []
     if not operatories :
        logger.error(f"No Operatories found for this calendar")
        return None
    
     DateTimeStart, DateTimeEnd , pattern = await  opendental_pattern_time_build (date_str , start_str, end_str, clinic_timezone = clinic_timezone)
     fmn = "%Y-%m-%d"
     fake_date_start  = datetime.strptime(DateTimeStart, fmt)
     real_date_start = datetime.strftime(fake_date_start, fmn)
     fake_date_end =  datetime.strptime(DateTimeEnd, fmt)
     real_date_end =  datetime.strftime(fake_date_end, fmn)
     for op in operatories:
            existing = await  od.get_appointments_operatory(op, real_date_start, real_date_end)
            #check if time_slot is full 
            if  await check_time_slot(existing ,DateTimeStart, DateTimeEnd ):
                print(f"creating appointment in {op} --booking appointment ")
                appointment = Appointments_create(
                    PatNum = pat_num,
                    Pattern = pattern,
                    AptDateTime =  DateTimeStart,
                    Op = op,
                    Note = Note,
                    AptStatus = status
                )

                if AptNum is not None :
                    appointment_update = Appointments_update(
                        Pattern  = pattern,
                        AptDateTime =  DateTimeStart, 
                        Note =  Note, 
                        Op = op, 
                        AptStatus =  status 
                    )
                    await od.update_appointment(AptNum, appointment_update)

                else:
                    await od.create_appointments(appointment_data = appointment ) 
            
            if commlogs:
                logs  = create_commslogs(
                    commlogs = commlogs,
                    PatNum = pat_num
                    )
                await od.create_commslog(logs)

            if create_popup:
                popup = create_pop_ups(
                    pop_ups = create_popup ,
                    PatNum = pat_num
                )
                await od.create_pop(popup)
                db.close()
                return (AptNum)
                
            else:
                print(" No free operatory found for that time slot.")










                ##############################GHL NORMALIZATION##############################
def create_contacts(data: create_contact_ghl):
    return{
        "firstName" : data.firstName,
        "lastName" : data.lastName,
        "Email" : data.email,
        "phone" : data.phone,
        "dateofBirth" : data.dateOfBirth
        }


def create_appointments (appt_data :  create_appointment_ghl):
    return {
        "calendarId" : appt_data.calendarId,
        "locationId" : appt_data.locationId,
        "contactId" : appt_data.contactId, 
        "startTime":  appt_data.startTime,
        "endTime":    appt_data.endTime,
        "ignoreFreeSlotValidation": appt_data.ignoreFreeSlotValidation, 
        "asignedUserId" : appt_data.assignedUserId,
        "appointmentStatus" : appt_data.appointmentStatus 
    }

def update_appointments (appt_data : update_appointment_ghl):
    return {
        "calendarId" : appt_data.calendarId,
        "locationId" : appt_data.locationId,
        "startTime":  appt_data.startTime,
        "endTime":    appt_data.endTime,
        "ignoreFreeSlotValidation": appt_data.ignoreFreeSlotValidation, 
        "asignedUserId" : appt_data.assignedUserId,
        "appointmentStatus" : appt_data.appointmentStatus 
    }






