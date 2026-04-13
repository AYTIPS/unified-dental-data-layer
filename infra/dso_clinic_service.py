from datetime import datetime, timedelta, time, timezone
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from fastapi import HTTPException, status
from sqlalchemy import String, cast, func, or_
from sqlalchemy.exc import  SQLAlchemyError
from sqlalchemy.orm import Session
from core.models import AppointmentSyncLog, RegisteredClinics, SyncStatus, RoleAssignment, ScopeType, RoleType
from core.schemas import dso_clinic_actions_out, dso_clinic_disabled_out, dso_clinic_summary_Out, dso_clinic_list_out, dso_clinic_row_out
from infra.dso_clinic_page_cache import DSO_CLINIC_LIST_TTL_SECONDS, cache_get_json,cache_set_json, dso_clinic_list_cache_Key, invalidate_dso_clinic_list_cache
from infra.rbac import get_clinic_role, get_dso_role



def today_window()-> tuple[datetime, datetime]:
    today= datetime.now(timezone.utc).date()
    start = datetime.combine(today, time.min, tzinfo= timezone.utc)
    end = start + timedelta(days= 1)
    return start, end 

def to_clinic_timezone(value: datetime | None, clinic_timezone: str)-> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo = timezone.utc)


