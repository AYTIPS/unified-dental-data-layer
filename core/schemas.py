from pydantic import BaseModel, EmailStr, ConfigDict, StringConstraints
from datetime import datetime
from typing import Optional
from typing import Literal, Annotated


Datestr = Annotated[str, StringConstraints(pattern=r"^\d{4}-\d{2}-\d{2}$")]



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
    Email: EmailStr 
    PriProv:str



class patient_model(BaseModel):
    contact_id : str
    FName :str
    LName: str
    Gender: str  
    Address: str 
    Birthdate: str 
    WirelessPhone:str 
    Email: Optional[EmailStr] 
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



###########################################  GHL  WORKKS 

class create_contact_ghl(BaseModel):
    firstName: str 
    lastName:str
    email: EmailStr
    phone:str 
    dateOfBirth: Datestr

class create_appointment_ghl(BaseModel):
    calendarId: str 
    locationId: str 
    contactId : str 
    startTime : str 
    endTime : str 
    ignoreFreeSlotValidation : Literal[True]
    assignedUserId : str 
    appointmentStatus : str 
    

class update_appointment_ghl(BaseModel):
    calendarId: str 
    locationId: str  
    startTime : str 
    endTime : str 
    ignoreFreeSlotValidation : Literal[True]
    assignedUserId : str 
    appointmentStatus : str 





################Authentication Schema 
class loginresponse(BaseModel):
    access_token : str 
    refresh_token : str 

class loginrequest(BaseModel):
    email : EmailStr
    password: str

class logoutresponse(BaseModel):
    message : str 

##################################UserRegistration 
class usercreate(BaseModel):
    username:str 
    email : EmailStr
    password : str
    username:str 

class userout(BaseModel):
    id : str 
    email : EmailStr 
    username : str 

    class config:
        orm_mode = True 