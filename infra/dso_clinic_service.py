from datetime import datetime, timedelta, time, timezone
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from fastapi import HTTPException, status
from sqlalchemy import String, cast, func, or_
from sqlalchemy.exc import  SQLAlchemyError
from sqlalchemy.orm import Session
from core.models import AppointmentSyncLog, RegisteredClinics, SyncStatus
