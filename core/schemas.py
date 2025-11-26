from pydantic import BaseModel, EmailStr, ConfigDict
from datetime import datetime
from typing import Optional
from typing import Literal


class Webhook_requests(BaseModel):
    event_id : str
    contact_id : str 
    commslog: str
    status : str
    Date : str 
    start_time : str
    end_time: str   
    first_name: str 
    last_name : str 
    BirthDate: str 
    Gender: str 
    Notes: Optional[str] = None 
    pop_up: Optional[str] = None 
    calendar_id : str 
    Note : str 
    WirelessPhone:str 
    Email: str 
    PriProv:str



class patient_model(BaseModel):
    contact_id : str
    FName :str
    LName: str
    Gender: str  
    Address: str 
    Birthdate: str 
    WirelessPhone:str 
    Email: Optional[str] 
    position : Optional[str]


class Appointments_create(BaseModel):
    PatNum:int
    Pattern: str 
    AptDateTime : str
    Op: str
    AptStatus: str
    Note: Optional[str] = None

class Appointments_update(  BaseModel):
    Pattern : str 
    AptDateTime : str 
    Note : Optional[str] = None
    Op : str 
    AptStatus : str 

class create_commslogs(BaseModel):
    commlogs : str 
    PatNum : str 

class create_pop_ups(BaseModel):
    pop_ups: str 
    PatNum :str 


