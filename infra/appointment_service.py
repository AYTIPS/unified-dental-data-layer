from sdk.opendental_sdk import openDentalApi
from fastapi import Depends
from  sqlalchemy.orm  import Session
from core.models import Appointments
from core.schemas import AppointmentRequest, Appointments_create, Appointments_update, create_commslogs, create_pop_ups
import logging 
from core.utils import check_time_slot, opendental_get_operatory_status, opendental_pattern_time_build

log = logging.getLogger(__name__)


class AppointmentService():
    def __init__(self, db: Session, clinic , od_client: openDentalApi) : 
        self.od = od_client
        self.db = db
        self.clinic = clinic

    async def book (self, req: AppointmentRequest ):
        AptNum = await self.get_existing_aptnum_from_db(req)
        operatories = await self.get_operatories(req)

        if not operatories:
            log.warning("No Operatories found for this Calendar ")
            return None 
    
        start_dt, end_dt , pattern = await self.build_time(req)

        AptNum = await self.book_into_operatory( req, operatories = operatories , start_dt = start_dt , end_dt = end_dt, pattern = pattern, AptNum = AptNum)

        if not AptNum:
            log.warning("No timeslot available for this Appointment in any operatory")
            return None 
        
        await self.handle_commslog(req)
        await self.handle_popups(req)

    async def book_into_operatory(self,  req: AppointmentRequest, operatories: list , start_dt, end_dt,  pattern : str  ,  AptNum : int | None   ):
        #Create date pattern for date time start and date time end
        date_start = start_dt.strftime("%Y-%m-%d")
        date_end = end_dt.strftime("%Y-%m-%d")

        for op in operatories: 
            existing = await self.od.get_appointments_in_operatory(op, date_start, date_end)
        
        
            if not await check_time_slot(existing, start_dt, end_dt):
                continue

            if AptNum: 
                log.info(f"Updated Appointment for Aptnum {AptNum} in  Op  {op}")
                await self.update_appointment( 
                    pattern = pattern, 
                    req = req ,
                    op = op,  
                    start_dt = start_dt,
                    AptNum= AptNum
                    )
            else: 
                log.info(f"created  Appointment for Aptnum {AptNum} in  Op  {op}")
                await self.create_appointment(
                    pattern = pattern,
                    op = op,
                    req = req,  
                    start_dt = start_dt 
                )

            return AptNum
        return None 

    async def create_appointment( self , req: AppointmentRequest, pattern, op, start_dt):
        appointment = Appointments_create(
            PatNum= req.pat_Num,
            Pattern = pattern,
            AptDateTime= start_dt, 
            Op = op,
            Note = req.Note,
            AptStatus = req.status
        )
        return self.od.create_appointments( appointment_data = appointment)
    
    async def update_appointment(self, req: AppointmentRequest, pattern, AptNum, op, start_dt): 
        appointment = Appointments_update(
            Pattern= pattern,
            AptDateTime= start_dt,
            Op = op,
            AptStatus= req.status
        )

        return self.od.update_appointment(Aptnum= AptNum, appointment_data = appointment)
    
    async def handle_commslog(self, req : AppointmentRequest):
        if not req.commslog:
            return 
        
        logs = create_commslogs(
            commlogs = req.commslog,
            PatNum = req.pat_Num 
        )
        await self.od.create_commslog(comms_logs = logs )
    

    
    async def handle_popups(self, req: AppointmentRequest):
        if not req.pop_up:
            return 
        
        pops = create_pop_ups(
            pop_ups = req.pop_up,
            PatNum = req.pat_Num
        )

        await self.od.create_pops(pops = pops )
    

    
    async def  get_existing_aptnum_from_db(self, req: AppointmentRequest):
        if not req.event_id:
            return None 
        
        existing = self.db.query(Appointments).filter_by(clinic_id = self.clinic.id, event_id = req.event_id).first()
        if not existing :
            return None 
        
        return int(existing.AptNum) # type: ignore


    async def  get_operatories(self, req: AppointmentRequest):
        return (
            await opendental_get_operatory_status(self.clinic, req.status, req.calendar_id)
        ) or [] 
    

    async def build_time(self, req: AppointmentRequest):
        return (
            await  opendental_pattern_time_build(req.date_str, req.start_str, req.end_str, req.clinic_timezone)
        )