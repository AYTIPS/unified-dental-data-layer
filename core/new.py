import json
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import hashlib
import re
import uuid
from typing import Dict, Tuple, Optional
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from cachetools import TTLCache
from ratelimit import limits, sleep_and_retry
from threading import Lock
import asyncio  # async idempotency locks
import base64
from io import BytesIO

# === Google Drive client (service account) =========================
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# === NEW: Google Chat Webhook URL ==================================
GOOGLE_CHAT_WEBHOOK_URL = os.getenv("GOOGLE_CHAT_WEBHOOK_URL")

# === NEW: Google Chat failure notifier =============================
async def notify_chat_failure(entity: str,
                              last_name: str,
                              first_name: str,
                              birth_date: str,
                              appt_start: str,
                              appt_end: str,
                              reason: str,
                              user:str,
                              request_id: str = "N/A") -> None:
    """
    Notify Google Chat when patient or appointment operations fail.
    entity: "Patient creation", "Patient update", "Appointment creation", "Appointment update"
    """
    if not GOOGLE_CHAT_WEBHOOK_URL:
        logger.warning("GOOGLE_CHAT_WEBHOOK_URL not set, skipping chat notification",
                       extra={'request_id': request_id})
        return
    try:
        text = (
            f"⚠️ {entity} failed.\n"
            f"Name: {first_name} {last_name}\n"
            f"DOB: {birth_date}\n"
            f"Appt: {appt_start} → {appt_end}\n"
            f"user: {user}\n"
            f"Reason: {reason}"
        )
        async with httpx.AsyncClient() as client:
            resp = await client.post(GOOGLE_CHAT_WEBHOOK_URL, json={"text": text})
            resp.raise_for_status()
        logger.info(f"Posted {entity} failure notification to Google Chat",
                    extra={'request_id': request_id})
    except Exception as e:
        logger.error(f"Failed to post failure notification to Google Chat: {str(e)}",
                     extra={'request_id': request_id})


def _load_sa_credentials():
    """
    Load service account credentials from one of:
    - GOOGLE_SA_JSON (raw JSON)
    - GOOGLE_SA_JSON_B64 (base64-encoded JSON)
    - GOOGLE_SA_JSON_PATH (file path)
    """
    scopes = os.getenv("GOOGLE_DRIVE_SCOPES", "https://www.googleapis.com/auth/drive").split(",")
    raw = os.getenv("GOOGLE_SA_JSON")
    b64 = os.getenv("GOOGLE_SA_JSON_B64")
    path = os.getenv("GOOGLE_SA_JSON_PATH")
    info = None

    if raw:
        info = json.loads(raw)
    elif b64:
        info = json.loads(base64.b64decode(b64).decode("utf-8"))
    elif path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            info = json.load(f)
    else:
        raise RuntimeError("No service account credentials provided. Set GOOGLE_SA_JSON or GOOGLE_SA_JSON_B64 or GOOGLE_SA_JSON_PATH.")

    return service_account.Credentials.from_service_account_info(info, scopes=scopes)

def _drive_service():
    creds = _load_sa_credentials()
    return build("drive", "v3", credentials=creds, cache_discovery=False)

async def drive_load_json_file(file_id: str, request_id: str) -> dict:
    """
    Download a JSON file from Drive by fileId and return dict.
    """
    def _download():
        service = _drive_service()
        req = service.files().get_media(fileId=file_id)
        buf = BytesIO()
        downloader = MediaIoBaseDownload(buf, req)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        buf.seek(0)
        content = buf.read().decode("utf-8") or "{}"
        try:
            data = json.loads(content)
            if isinstance(data, dict):
                return data
            # If file is an array, convert to dict wrapper
            return {"data": data}
        except json.JSONDecodeError:
            return {}
    try:
        return await asyncio.to_thread(_download)
    except Exception as e:
        logger.error(f"[Drive] load failed for file {file_id}: {str(e)}", extra={'request_id': request_id})
        return {}

async def drive_save_json_file(file_id: str, payload: dict, request_id: str) -> bool:
    """
    Upload (overwrite) a JSON file on Drive by fileId.
    """
    def _upload():
        service = _drive_service()
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        media = MediaIoBaseUpload(BytesIO(body), mimetype="application/json", resumable=True)
        # files().update keeps the same fileId
        service.files().update(fileId=file_id, media_body=media).execute()
        return True
    try:
        return await asyncio.to_thread(_upload)
    except Exception as e:
        logger.error(f"[Drive] save failed for file {file_id}: {str(e)}", extra={'request_id': request_id})
        return False

# A thin in-memory cache to reduce Drive calls during bursts.
_drive_map_cache = {"data": {}, "ts": 0}
_drive_map_lock = asyncio.Lock()
DRIVE_CACHE_TTL_SEC = 8

async def event_map_load(file_id: str, request_id: str) -> dict:
    """
    Load the event→apt mapping dict from Drive (with small TTL cache).
    Shape:
      {
        "EVENT_ID_123": {
          "aptNum": "12345",
          "patNum": "67890",
          "clinicNum": "9034",
          "lastDate": "2025-09-25",
          "contactId": "abcdef",
          "calendar": "Celebrate Dental ...",
          "updatedAt": "2025-09-25T15:11:22Z"
        },
        ...
      }
    """
    async with _drive_map_lock:
        now = datetime.utcnow().timestamp()
        if (now - _drive_map_cache["ts"]) <= DRIVE_CACHE_TTL_SEC:
            return _drive_map_cache["data"]
        data = await drive_load_json_file(file_id, request_id) or {}
        if not isinstance(data, dict):
            data = {}
        _drive_map_cache["data"] = data
        _drive_map_cache["ts"] = now
        return data

async def event_map_save(file_id: str, mapping: dict, request_id: str) -> bool:
    """
    Persist the mapping dict to Drive and refresh cache.
    """
    async with _drive_map_lock:
        ok = await drive_save_json_file(file_id, mapping, request_id)
        if ok:
            _drive_map_cache["data"] = mapping
            _drive_map_cache["ts"] = datetime.utcnow().timestamp()
        return ok

# === App & original code =====================================================

app = FastAPI()

# Custom logging formatter
class CustomFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'request_id'):
            record.request_id = 'N/A'
        return super().format(record)

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = CustomFormatter('{"time": "%(asctime)s", "level": "%(levelname)s", "request_id": "%(request_id)s", "message": "%(message)s"}')
file_handler = RotatingFileHandler('webhook.log', maxBytes=10*1024*1024, backupCount=5)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# Environment variables
DEVELOPER_KEY = os.getenv('DEVELOPER_KEY')
CUSTOMER_KEY = os.getenv('CUSTOMER_KEY')
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET')
CALENDAR_TO_CLINIC_MAP = json.loads(os.getenv('CALENDAR_TO_CLINIC_MAP', '{}'))
CALENDAR_TO_OP_MAP = json.loads(os.getenv('CALENDAR_TO_OP_MAP', '{}'))
OPEN_DENTAL_API_URL = os.getenv('OPEN_DENTAL_API_URL', 'https://api.opendental.com/api/v1')
GOHIGHLEVEL_API_URL = os.getenv('GHL_API_URL', 'https://services.leadconnectorhq.com')
GOHIGHLEVEL_API_KEY = os.getenv('GHL_API_KEY', '')

# NEW: Google Drive mapping file env
GOOGLE_DRIVE_FILE_ID = os.getenv('GOOGLE_DRIVE_FILE_ID')

# Load appointment types
APPOINTMENT_TYPE_MAP = {
    'cash consult': 328,
    'comp ex': 344,
    'comp ex child': 345,
    'insurance consult': 329,
    'ins consult': 140
}
try:
    with open('appointment_types.json', 'r') as f:
        file_appointment_types = json.load(f)
        temp_map = {}
        if 'appointment_types' in file_appointment_types and isinstance(file_appointment_types['appointment_types'], dict) and 'shared' in file_appointment_types['appointment_types']:
            shared_types = file_appointment_types['appointment_types']['shared']
            for k, v in shared_types.items():
                if not v or not isinstance(v, str):
                    logger.error(f"Invalid appointment type name for ID {k}: {v}, skipping entry", extra={'request_id': 'N/A'})
                    continue
                try:
                    temp_map[v.lower().strip()] = int(k)
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid appointment type ID for name {v}: {k}, error: {str(e)}, skipping entry", extra={'request_id': 'N/A'})
                    continue
        else:
            for k, v in file_appointment_types.items():
                try:
                    temp_map[k.lower().strip()] = int(v)
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid appointment type value for key {k}: {v}, error: {str(e)}, skipping entry", extra={'request_id': 'N/A'})
                    continue
        if temp_map:
            APPOINTMENT_TYPE_MAP = temp_map
            logger.info(f"Loaded {len(APPOINTMENT_TYPE_MAP)} appointment types from file", extra={'request_id': 'N/A'})
        else:
            logger.warning("No valid entries in appointment_types.json, using default APPOINTMENT_TYPE_MAP", extra={'request_id': 'N/A'})
except FileNotFoundError:
    logger.warning("appointment_types.json not found, using default APPOINTMENT_TYPE_MAP", extra={'request_id': 'N/A'})
except json.JSONDecodeError:
    logger.error("Invalid JSON in appointment_types.json, using default APPOINTEMENT_TYPE_MAP", extra={'request_id': 'N/A'})
except Exception as e:
    logger.error(f"Failed to load appointment_types.json: {str(e)}, using default APPOINTMENT_TYPE_MAP", extra={'request_id': 'N/A'})

# Caches
patient_cache = TTLCache(maxsize=1000, ttl=3600)
appointment_cache = TTLCache(maxsize=1000, ttl=40)
provider_cache = TTLCache(maxsize=100, ttl=86400)
cache_lock = Lock()
_referrals_def_ready: bool = False

# Async idempotency locks (avoid event-loop blocking)
_inflight_global_lock = asyncio.Lock()
_inflight_locks: Dict[str, asyncio.Lock] = {}
async def _get_dedupe_lock(key: str) -> asyncio.Lock:
    async with _inflight_global_lock:
        if key not in _inflight_locks:
            _inflight_locks[key] = asyncio.Lock()
        return _inflight_locks[key]

# NEW: per-event lock to serialize Drive mapping updates by event id
_event_locks: Dict[str, asyncio.Lock] = {}
async def _get_event_lock(event_id: str) -> asyncio.Lock:
    async with _inflight_global_lock:
        if event_id not in _event_locks:
            _event_locks[event_id] = asyncio.Lock()
        return _event_locks[event_id]

# Rate limiting
CALLS = 100
PERIOD = 60

# Mappings
STATUS_MAP = {
    'showed': 'Complete',
    'cancelled': 'Broken',
    'confirmed': 'Scheduled',
    'booked': 'Scheduled',
    '': 'Scheduled'
}
GENDER_MAP = {
    'M': 'Male',
    'F': 'Female',
    'Male': 'Male',
    'Female': 'Female',
    'Other': 'Unknown',
    '': 'Unknown'
}
CLINIC_TO_PROVNUM_MAP = {
    '9034': '17257',
    '9035': '17250'
}

def make_auth_header() -> Dict[str, str]:
    return {
        'Authorization': f'ODFHIR {DEVELOPER_KEY}/{CUSTOMER_KEY}',
        'Content-Type': 'application/json'
    }

def normalize_field_name(field: str) -> str:
    return field.strip().lower() if isinstance(field, str) else ''

def normalize_calendar_name(calendar: str) -> str:
    if not isinstance(calendar, str):
        return ''
    calendar = calendar.strip()
    if '{{' in calendar and '}}' in calendar:
        logger.warning(f"Detected placeholder in calendar name: {calendar}", extra={'request_id': 'N/A'})
        return ''
    calendar_variations = {
        'celebrate dental austin gp': 'Celebrate Dental Austin Appointments (GP Treatments)',
        'celebrate dental austin orthodontic': 'Celebrate Dental Austin Appointments (Orthodontic Treatments)',
        'gp treatments': 'Celebrate Dental Austin Appointments (GP Treatments)',
        'orthodontic treatments': 'Celebrate Dental Austin Appointments (Orthodontic Treatments)',
        'celebrate dental austin appointments (orthodontic treatments)': 'Celebrate Dental Austin Appointments (Orthodontic Treatments)',
        'celebrate dental austin appointments (gp treatments)': 'Celebrate Dental Austin Appointments (GP Treatments)',
        '(ortho) austin appts': 'Celebrate Dental Austin Appointments (Orthodontic Treatments)',
        '(gp) austin appts': 'Celebrate Dental Austin Appointments (GP Treatments)'
    }
    return calendar_variations.get(calendar.lower(), calendar)

def validate_and_parse_date(date_str: str, field_name: str, request_id: str = 'N/A') -> Optional[str]:
    try:
        if not date_str or not isinstance(date_str, str):
            logger.error(f"Missing or invalid date for {field_name}: {date_str}", extra={'request_id': request_id})
            return None
        date_str = date_str.strip()
        patterns = [
            r'^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})$',
            r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})(st|nd|rd|th)?\s+(\d{4})$'
        ]
        matched = False
        for pattern in patterns:
            if re.match(pattern, date_str, re.IGNORECASE):
                matched = True
                break
        if not matched:
            logger.error(f"Invalid date format for {field_name}: {date_str}, expected 'Month DD, YYYY' or 'MMM DDth YYYY'", extra={'request_id': request_id})
            return None
        try:
            dt = datetime.strptime(date_str, '%B %d, %Y')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass
        patterns = [
            '%b %dth %Y', '%b %dst %Y', '%b %dnd %Y', '%b %drd %Y',
            '%B %dth %Y', '%B %dst %Y', '%B %dnd %Y', '%B %drd %Y'
        ]
        for pattern in patterns:
            try:
                dt = datetime.strptime(date_str, pattern)
                if dt.day < 1 or dt.day > 31:
                    logger.error(f"Invalid day in date for {field_name}: {date_str}", extra={'request_id': request_id})
                    return None
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        logger.error(f"Failed to parse date for {field_name}: {date_str}", extra={'request_id': request_id})
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing date for {field_name}: {date_str}, error: {str(e)}", extra={'request_id': request_id})
        return None

def validate_and_parse_time(time_str: str, date_str: str, field_name: str, request_id: str = 'N/A') -> Optional[str]:
    try:
        if not time_str or not isinstance(time_str, str):
            logger.error(f"Missing or empty time for {field_name}: {time_str}", extra={'request_id': request_id})
            return None
        if not date_str or not isinstance(date_str, str):
            logger.error(f"Missing or invalid date for {field_name}: {date_str}", extra={'request_id': request_id})
            return None
        time_str = time_str.strip()
        date_str = date_str.strip()
        try:
            dt = datetime.strptime(f"{date_str} {time_str}", '%B %d, %Y %I:%M %p')
            dt = dt.replace(tzinfo=ZoneInfo('America/Chicago'))
            dt_utc = dt.astimezone(ZoneInfo('UTC'))
            logger.info(f"Parsed {field_name}: {time_str} on {date_str} to UTC: {dt_utc.isoformat()}", extra={'request_id': request_id})
            return dt_utc.isoformat()
        except ValueError as e:
            logger.error(f"Invalid time or date format for {field_name}: {time_str} on {date_str}, expected 'Month DD, YYYY HH:MM AM/PM', error: {str(e)}", extra={'request_id': request_id})
            return None
    except Exception as e:
        logger.error(f"Unexpected error parsing time for {field_name}: {time_str} on {date_str}, error: {str(e)}", extra={'request_id': request_id})
        return None

async def validate_webhook_secret(request: Request, request_id: str) -> bool:
    if not WEBHOOK_SECRET:
        logger.warning("WEBHOOK_SECRET not set, skipping validation", extra={'request_id': request_id})
        return True
    signature = request.headers.get('x-secret')
    if not signature:
        logger.warning("Missing webhook signature, allowing request for testing", extra={'request_id': request_id})
        return True
    if signature != WEBHOOK_SECRET:
        logger.error("Invalid webhook signature", extra={'request_id': request_id})
        return False
    return True

async def get_clinic_num(calendar_name: str, request_id: str = 'N/A') -> Optional[str]:
    try:
        normalized_calendar = normalize_calendar_name(calendar_name)
        clinic_num = CALENDAR_TO_CLINIC_MAP.get(normalized_calendar)
        if not clinic_num:
            logger.error(f"No ClinicNum mapped for calendar: {normalized_calendar}", extra={'request_id': request_id})
            return None
        logger.info(f"Retrieved ClinicNum: {clinic_num} for calendar: {normalized_calendar}", extra={'request_id': request_id})
        return clinic_num
    except Exception as e:
        logger.error(f"Error retrieving ClinicNum for calendar: {calendar_name}, error: {str(e)}", extra={'request_id': request_id})
        return None

def calculate_pattern(start_time: str, end_time: str, request_id: str = 'N/A') -> Optional[str]:
    try:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        duration_minutes = int((end_dt - start_dt).total_seconds() / 60)
        if duration_minutes <= 0 or duration_minutes % 5 != 0:
            logger.error(f"Invalid duration: {duration_minutes} minutes (must be positive and divisible by 5)", extra={'request_id': request_id})
            return None
        num_intervals = duration_minutes // 5
        return 'X' * num_intervals
    except Exception as e:
        logger.error(f"Error calculating pattern: {str(e)}", extra={'request_id': request_id})
        return None

# The open dental format for languages is ISO 639-2 format
def convert_language_to_od_format(language_ghl_format: str) -> str:
    primary_language_ghl_format = language_ghl_format.split(",")

    if len(primary_language_ghl_format) == 0:
        return "invalid"

    primary_language_ghl_format = primary_language_ghl_format[0]

    match primary_language_ghl_format:
        case "English":
            return "eng"
        case "Spanish":
            return "spa"
        case "":
            return "invalid"
        case "None":
            return "invalid"
        case _:
            return ""
        
async def get_pat_num_of_patient(request_id, clinic_num, last_name, first_name, birth_date) -> Optional[int]:
    # STRICT patient lookup
    patient_key = hashlib.md5(f"{clinic_num}:{_norm_name(last_name)}:{_norm_name(first_name)}:{birth_date}".encode()).hexdigest()
    pat_num = None
    with cache_lock:
        if patient_key in patient_cache:
            pat_num = patient_cache[patient_key]
            logger.info(f"Cache hit for patient: {last_name}/{first_name}, PatNum: {pat_num}", extra={'request_id': request_id})
    if not pat_num:
        patient_params = {'LName': last_name, 'Birthdate': birth_date, 'clinicNums': clinic_num}
        if first_name:
            patient_params['FName'] = first_name
        try:
            logger.info(f"Querying patients with params: {patient_params}", extra={'request_id': request_id})
            patient_response = await make_api_request('GET', '/patients', request_id, params=patient_params)
            logger.info(f"Patient query response: {json.dumps(patient_response, indent=2)}", extra={'request_id': request_id})
            def _matches(p):
                res = p.get('resource', p)
                lname = _norm_name(res.get('LName', ''))
                fname = _norm_name(res.get('FName', ''))
                dob = (res.get('Birthdate') or res.get('BirthDate') or '')[:10]
                if dob != birth_date:
                    return False
                if lname != _norm_name(last_name):
                    return False
                if first_name and fname != _norm_name(first_name):
                    return False
                return True
            match = None
            if isinstance(patient_response, list):
                for entry in patient_response:
                    if _matches(entry):
                        match = entry.get('resource', entry)
                        break
            if match:
                pat_num = match['PatNum']
                with cache_lock:
                    patient_cache[patient_key] = pat_num
                logger.info(f"Exact patient match PatNum: {pat_num}.", extra={'request_id': request_id})
            else:
                logger.info("No exact patient match; will create new patient.", extra={'request_id': request_id})
        except Exception as e:
            logger.error(f"Failed to query /patients: {str(e)}", extra={'request_id': request_id})
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Patient query failed: {str(e)}")
    return pat_num

# --- status grouping helper
def _status_group(apt_status: str) -> str:
    if apt_status == 'Broken':
        return 'cancelled'
    if apt_status == 'Complete':
        return 'completed'
    return 'scheduled'

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, max=32), retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.RequestError)))
async def make_api_request(method: str, endpoint: str, request_id: str, data: dict = None, params: dict = None) -> dict:
    async with httpx.AsyncClient() as client:
        try:
            headers = make_auth_header()
            url = f"{OPEN_DENTAL_API_URL}{endpoint}"
            logger.info(f"Making {method} request to {url} with params: {params}, data: {data}", extra={'request_id': request_id})
            response = await client.request(method, url, headers=headers, json=data, params=params, timeout=5)
            response.raise_for_status()
            logger.info(f"Successful {method} request to {url}, status code: {response.status_code}", extra={'request_id': request_id})
            return response.json() if response.content else {}
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (400, 404) and endpoint == '/patients' and method == 'GET':
                logger.info(f"No patient found for {params} (status: {e.response.status_code}, response: {e.response.text})", extra={'request_id': request_id})
                return {'total': 0}
            if e.response.status_code in (400, 404) and endpoint == '/patients' and method == 'POST':
                logger.error(f"Failed to create patient (status: {e.response.status_code}, response: {e.response.text})", extra={'request_id': request_id})
                raise Exception(f"Patient creation failed: {e.response.text}")
            if e.response.status_code == 401:
                logger.error(f"Unauthorized error for {url}: {str(e)}, response: {e.response.text}", extra={'request_id': request_id})
                raise
            if e.response.status_code == 429:
                logger.error(f"Rate limit exceeded for {url}: {str(e)}, response: {e.response.text}")
                raise Exception("Rate limit exceeded")
            logger.error(f"HTTP error for {url}: {str(e)}, response: {e.response.text}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request failed for {url}: {str(e)}", extra={'request_id': request_id})
            raise

async def get_default_provider(clinic_num: str, request_id: str) -> Optional[str]:
    try:
        cache_key = f"provider:{clinic_num}"
        with cache_lock:
            if cache_key in provider_cache:
                logger.info(f"Cache hit for provider: ClinicNum: {clinic_num}, ProvNum: {provider_cache[cache_key]}", extra={'request_id': request_id})
                return provider_cache[cache_key]
        provider_response = await make_api_request('GET', '/providers', request_id, params={'ClinicNum': clinic_num})
        logger.info(f"Provider query response for ClinicNum: {clinic_num}: {json.dumps(provider_response, indent=2)}", extra={'request_id': request_id})
        if isinstance(provider_response, list) and len(provider_response) > 0:
            provider = provider_response[0].get('resource', provider_response[0])
            prov_num = provider.get('ProvNum')
            with cache_lock:
                provider_cache[cache_key] = prov_num
            logger.info(f"Selected default ProvNum: {prov_num} for ClinicNum: {clinic_num}", extra={'request_id': request_id})
            return prov_num
        logger.error(f"No providers found for ClinicNum: {clinic_num}", extra={'request_id': request_id})
        return None
    except Exception as e:
        logger.error(f"Failed to fetch providers for ClinicNum: {clinic_num}: {str(e)}", extra={'request_id': request_id})
        return None

async def _find_same_day_appt_in_ops(clinic_num: str, pat_num: str, apt_datetime: str, allowed_ops, request_id: str):
    try:
        date_str = apt_datetime.split('T')[0]
        params = {'ClinicNum': clinic_num, 'PatNum': pat_num, 'dateStart': date_str, 'dateEnd': date_str}
        resp = await make_api_request('GET', '/appointments', request_id, params=params)
        allow_set = {str(o) for o in (allowed_ops or []) if o is not None}
        if isinstance(resp, list):
            # Prefer exact time match in allowed ops
            for item in resp:
                r = item.get('resource', item)
                if str(r.get('Op')) in allow_set and r.get('AptDateTime') == apt_datetime:
                    return r.get('AptNum'), r.get('Op')
            # Fallback: any same-day in allowed ops
            for item in resp:
                r = item.get('resource', item)
                if str(r.get('Op')) in allow_set:
                    return r.get('AptNum'), r.get('Op')
    except Exception as e:
        logger.error(f"Failed same-day search in ops {allowed_ops} for PatNum={pat_num}: {str(e)}", extra={'request_id': request_id})
    return None, None

async def get_operatory(calendar_name: str, status: str, apt_datetime: str, clinic_num: str, pat_num: str, request_id: str) -> Optional[str]:
    try:
        normalized_calendar = normalize_calendar_name(calendar_name)
        op_map = CALENDAR_TO_OP_MAP.get(normalized_calendar)
        if not op_map:
            logger.error(f"No operatory mapping for calendar: {normalized_calendar}", extra={'request_id': request_id})
            return None

        group = _status_group(status)  # 'scheduled' | 'completed' | 'cancelled'
        logger.info(f"Selecting operatory group='{group}' for status='{status}' on calendar '{normalized_calendar}'", extra={'request_id': request_id})

        if group == 'cancelled':
            op = op_map.get('cancelled')
            if not op:
                logger.error(f"No cancelled operatory defined for calendar: {normalized_calendar}", extra={'request_id': request_id})
                return None
            logger.info(f"Assigned operatory {op} for Broken in calendar: {normalized_calendar}", extra={'request_id': request_id})
            return op

        available_ops = op_map.get(group, [])
        if not available_ops:
            logger.error(f"No {group} operatories defined for calendar: {normalized_calendar}", extra={'request_id': request_id})
            return None

        logger.info(f"Checking available operatories {available_ops} for group {group} in calendar: {normalized_calendar}", extra={'request_id': request_id})

        # clear any cached responses for this time
        for op in available_ops:
            cache_key = hashlib.md5(f"{clinic_num}:{apt_datetime}:{op}".encode()).hexdigest()
            with cache_lock:
                if cache_key in appointment_cache:
                    del appointment_cache[cache_key]
                    logger.info(f"Cleared cache for ClinicNum: {clinic_num}, operatory {op} at {apt_datetime}", extra={'request_id': request_id})

        async def check_operatory(op):
            try:
                cache_key = hashlib.md5(f"{clinic_num}:{apt_datetime}:{op}".encode()).hexdigest()
                with cache_lock:
                    if cache_key in appointment_cache:
                        logger.info(f"Cache hit for ClinicNum: {clinic_num}, operatory {op} at {apt_datetime}", extra={'request_id': request_id})
                        return op, appointment_cache[cache_key]
                conflict_params = {
                    'ClinicNum': clinic_num,
                    'dateStart': apt_datetime.split('T')[0],
                    'dateEnd': apt_datetime.split('T')[0],
                    'Op': op,
                    'AptDateTime': apt_datetime
                }
                conflict_response = await make_api_request('GET', '/appointments', request_id, params=conflict_params)
                logger.info(f"Availability check for operatory {op} at {apt_datetime}: {json.dumps(conflict_response, indent=2)}", extra={'request_id': request_id})
                with cache_lock:
                    appointment_cache[cache_key] = conflict_response
                return op, conflict_response
            except Exception as e:
                logger.error(f"Failed to query /appointments for operatory {op}: {str(e)}", extra={'request_id': request_id})
                return op, None

        # === UPDATED LOGIC STARTS HERE ===
        if group == "scheduled":
            op_a = available_ops[0] if len(available_ops) > 0 else None
            op_b = available_ops[1] if len(available_ops) > 1 else None

            if not op_a:
                logger.error(f"No operatories found for scheduled group in calendar: {normalized_calendar}", extra={"request_id": request_id})
                return None

            logger.info(
                f"Checking available operatories {available_ops} for group scheduled in calendar: {normalized_calendar}",
                extra={"request_id": request_id}
            )

            # ---- Fetch all appointments for A and B ----
            ops_to_check = [op_a] + ([op_b] if op_b else [])
            results = {}

            for op_id in ops_to_check:
                result = await check_operatory(op_id)
                if result and isinstance(result[1], list):
                    results[op_id] = result[1]
                else:
                    results[op_id] = []

            # ---- Compare specific time ----
            def is_slot_taken(appointments: list, target_dt_local: str) -> bool:
                """Compare OpenDental UTC times with webhook time converted to UTC."""
                try:
                    clinic_tz = ZoneInfo("America/Chicago")

                    # webhook start time (local Chicago)
                    target_start_local = datetime.fromisoformat(target_dt_local.replace("Z", "+00:00"))
                    target_start_local = target_start_local.astimezone(clinic_tz)

                    # default 60-minute duration if not otherwise specified
                    target_end_local = target_start_local + timedelta(minutes=60)

                except Exception as e:
                    logger.warning(f"Invalid target datetime format: {target_dt_local} ({e})")
                    return False

                # iterate over all OpenDental appointments
                for appt in appointments:
                    od_time_str = str(appt.get("AptDateTime", ""))
                    if not od_time_str:
                        continue

                    try:
                        # parse OpenDental start time (local)
                        od_start = datetime.fromisoformat(od_time_str)
                        if od_start.tzinfo is None:
                            od_start = od_start.replace(tzinfo=clinic_tz)

                        # compute duration from Pattern (each slot = 5 minutes)
                        pattern = appt.get("Pattern", "")
                        duration_min = len(pattern) * 5 if pattern else 60
                        od_end = od_start + timedelta(minutes=duration_min)

                        # overlap condition
                        if target_start_local < od_end and od_start < target_end_local:
                            logger.info(
                                f"Overlap detected: Webhook {target_start_local.strftime('%H:%M')}–"
                                f"{target_end_local.strftime('%H:%M')} vs OD {od_start.strftime('%H:%M')}–"
                                f"{od_end.strftime('%H:%M')}",
                                extra={"request_id": request_id}
                            )
                            return True

                    except Exception:
                        continue

                return False

            # 1️⃣ Column A
            a_taken = is_slot_taken(results.get(op_a, []), apt_datetime)
            if not a_taken:
                logger.info(f"Slot {apt_datetime} free on A ({op_a}), booking there.", extra={"request_id": request_id})
                return op_a

            # 2️⃣ Column B
            if op_b:
                b_taken = is_slot_taken(results.get(op_b, []), apt_datetime)
                if not b_taken:
                    logger.info(f"Slot {apt_datetime} free on B ({op_b}), booking there.", extra={"request_id": request_id})
                    return op_b

            # 3️⃣ Both occupied → force A
            logger.warning(f"Slot {apt_datetime} taken on A and B, forcing into A ({op_a}).", extra={"request_id": request_id})
            return op_a
        # === UPDATED LOGIC ENDS HERE ===

        # default logic for completed (unchanged)
        for op in available_ops:
            result = await check_operatory(op)
            if result and isinstance(result[1], list) and len(result[1]) == 0:
                logger.info(f"Selected operatory {op} for PatNum: {pat_num} at {apt_datetime} in ClinicNum: {clinic_num} (available, group={group})", extra={'request_id': request_id})
                return op

        default_op = available_ops[0]
        logger.info(f"No available operatories in group {group} for PatNum: {pat_num} at {apt_datetime} in ClinicNum: {clinic_num}, defaulting to operatory {default_op}", extra={'request_id': request_id})
        return default_op

    except Exception as e:
        logger.error(f"Error in get_operatory: {str(e)}", extra={'request_id': request_id})
        return None

def _norm_name(s: str) -> str:
    if not isinstance(s, str):
        return ''
    return re.sub(r'[^a-z]', '', s.lower())

def _strip_fromghl_suffix(note: str) -> str:
    if not isinstance(note, str):
        return ''
    return re.sub(r'\s*\[fromghl\]\s*$', '', note.strip(), flags=re.IGNORECASE)

def _format_commdatetime_from_iso(iso_ts: str) -> Optional[str]:
    try:
        dt = datetime.fromisoformat(iso_ts.replace('Z', '+00:00'))
        local_dt = dt.astimezone(ZoneInfo('America/Chicago'))
        return local_dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return None

async def _create_commlog(pat_num: str, note: str, request_id: str, comm_datetime_iso: Optional[str] = None, apt_num: Optional[int | str] = None) -> None:
    try:
        cleaned_note = _strip_fromghl_suffix(note)
        if not cleaned_note:
            logger.info(f"Commlog not created: note empty after sanitization for PatNum={pat_num}", extra={'request_id': request_id})
            return

        payload = {
            "PatNum": pat_num,
            "Note": cleaned_note,
            "Mode_": "Text",
            "SentOrReceived": "Received",
            "commType": "ApptRelated"
        }
        resp = await make_api_request('POST', '/commlogs', request_id, data=payload)
        logger.info(f"Created commlog for PatNum={pat_num}. Response: {json.dumps(resp)}", extra={'request_id': request_id})
    except Exception as e:
        logger.error(f"Failed to create commlog for PatNum={pat_num}: {str(e)}", extra={'request_id': request_id})

async def _create_popup(pat_num: str, apt_num: str | int, note: str, request_id: str) -> None:
    try:
        popup_note = (note or "").strip()
        if not popup_note:
            logger.info(f"No popup note supplied; skipping popup creation for PatNum={pat_num}", extra={'request_id': request_id})
            return
        payload = {
            "PatNum": pat_num,
            "Description": popup_note,
            "PopupLevel": "Patient"
        }
        resp = await make_api_request('POST', '/popups', request_id, data=payload)
        logger.info(f"Created popup for PatNum={pat_num}. Response: {json.dumps(resp)}", extra={'request_id': request_id})
    except Exception as e:
        logger.error(f"Failed to create popup for PatNum={pat_num}: {str(e)}", extra={'request_id': request_id})

async def _upsert_patient_field_referral(pat_num: str, referral_text: str, request_id: str) -> None:
    try:
        if not isinstance(referral_text, str):
            return
        referral_name = referral_text.strip()
        if not referral_name:
            return

        try:
            existing = await make_api_request('GET', '/refattaches', request_id, params={'PatNum': pat_num})
            if isinstance(existing, list):
                for item in existing:
                    res = item.get('resource') or item
                    if str(res.get('PatNum')) != str(pat_num):
                        continue
                    ref_type = (res.get('ReferralType') or '').strip()
                    name = (res.get('referralName') or '').strip()
                    if ref_type == 'RefFrom' and name.lower() == referral_name.lower():
                        logger.info(f"Referral already attached (RefFrom) for PatNum={pat_num} with name='{referral_name}', skipping.", extra={'request_id': request_id})
                        return
        except Exception as e:
            logger.info(f"GET /refattaches failed (non-fatal): {str(e)}", extra={'request_id': request_id})

        payload = {"PatNum": pat_num, "referralName": referral_name, "ReferralType": "RefFrom"}
        try:
            resp = await make_api_request('POST', '/refattaches', request_id, data=payload)
            logger.info(f"Attached referral '{referral_name}' to PatNum={pat_num} (RefFrom). Response: {json.dumps(resp)}", extra={'request_id': request_id})
        except Exception as e:
            logger.error(f"POST /refattaches failed for PatNum={pat_num}, name='{referral_name}': {str(e)}", extra={'request_id': request_id})
    except Exception as e:
        logger.error(f"Referrals attach failed for PatNum={pat_num}: {str(e)}", extra={'request_id': request_id})

async def _resolve_proccode_num(proc_code: str, request_id: str) -> Optional[str]:
    try:
        resp = await make_api_request('GET', '/procedurecodes', request_id, params={'ProcCode': proc_code})
        if isinstance(resp, list) and resp:
            res = resp[0].get('resource', resp[0])
            return res.get('CodeNum')
    except Exception as e:
        logger.info(f"Could not resolve CodeNum for ProcCode={proc_code}: {str(e)}", extra={'request_id': request_id})
    return None

async def _has_procedure_for_appt(pat_num: str, apt_num: str, proc_code: str, request_id: str) -> bool:
    try:
        resp = await make_api_request('GET', '/procedurelogs', request_id, params={'PatNum': pat_num, 'AptNum': apt_num})
        if isinstance(resp, list):
            for item in resp:
                r = item.get('resource', item)
                code = (r.get('procCode') or r.get('ProcCode') or '').strip()
                if code.upper() == proc_code.upper():
                    return True
        return False
    except Exception as e:
        logger.info(f"GET /procedurelogs failed (non-fatal) for PatNum={pat_num}, AptNum={apt_num}: {str(e)}", extra={'request_id': request_id})
        return False

async def _get_created_apt_id(create_resp: dict, pat_num: str, clinic_num: str, apt_datetime: str, request_id: str) -> Optional[str]:
    try:
        created_apt = create_resp.get('resource', create_resp)
        apt_id = created_apt.get('AptNum') or created_apt.get('id') or created_apt.get('aptNum')
        if apt_id:
            return str(apt_id)
    except Exception:
        pass
    for i in range(3):
        try:
            await asyncio.sleep(0.3 * (i + 1))
            chk = await make_api_request('GET', '/appointments', request_id,
                                         params={'PatNum': pat_num, 'ClinicNum': clinic_num, 'AptDateTime': apt_datetime})
            if isinstance(chk, list) and chk:
                res = chk[0].get('resource', chk[0])
                apt_id = res.get('AptNum') or res.get('id') or res.get('aptNum')
                if apt_id:
                    return str(apt_id)
        except Exception as e:
            logger.info(f"Retry {i+1}/3 fetching AptNum after create failed: {str(e)}", extra={'request_id': request_id})
    logger.error("Unable to resolve AptNum for newly created appointment.", extra={'request_id': request_id})
    return None

async def _maybe_add_csc_procedure(
    pat_num: str,
    apt_num: str,
    clinic_num: str,
    prov_num: str,
    request_id: str,
    proc_date: Optional[str] = None
) -> None:
    try:
        proc_code = 'CCS'

        # 1️⃣ Skip if procedure already exists
        if await _has_procedure_for_appt(pat_num, apt_num, proc_code, request_id):
            logger.info(
                f"Procedure {proc_code} already attached to AptNum={apt_num}, skipping.",
                extra={'request_id': request_id}
            )
            return

        # 2️⃣ Build payload
        payload = {
            'PatNum': pat_num,
            'AptNum': int(apt_num),
            'ClinicNum': clinic_num,
            'ProvNum': prov_num,
            'ProcStatus': 'TP',
            'ProcDate': proc_date or datetime.utcnow().strftime('%Y-%m-%d'),
            'procCode': proc_code,   # use only procCode, not CodeNum
            'Note': 'Auto-added because CSC=Yes (GHL webhook).',
        }

        # 🚫 Do NOT add CodeNum — OpenDental rejects when both are given
        # If you ever need to switch to CodeNum-only mode, comment out procCode and use CodeNum instead.

        # 3️⃣ Make API request
        resp = await make_api_request('POST', '/procedurelogs', request_id, data=payload)
        logger.info(
            f"Attached procedure {proc_code} to AptNum={apt_num}, response: {json.dumps(resp)}",
            extra={'request_id': request_id}
        )

    except Exception as e:
        logger.error(
            f"Failed adding CSC procedure to PatNum={pat_num}, AptNum={apt_num}: {str(e)}",
            extra={'request_id': request_id}
        )


# --- helpers specific to Drive-backed mapping -------------------------------

def _now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def _extract_event_id(normalized: dict) -> Optional[str]:
    """
    Try several keys for event id coming from custom data: appointmentId / event id / appointment id / ghl_event_id.
    All normalized to lower-case keys already.
    """
    candidates = [
        "appointmentid",
        "event id",
        "eventid",
        "appointment id",
        "ghl_event_id",
        "ghl event id",
        "appointment_id",
        "event_id",
    ]
    for k in candidates:
        if k in normalized and normalized[k]:
            return str(normalized[k]).strip()
    return None

async def _fetch_appointment(apt_num: str, request_id: str) -> Optional[dict]:
    try:
        resp = await make_api_request('GET', f'/appointments/{apt_num}', request_id)
        if isinstance(resp, dict):
            return resp.get('resource', resp)
    except Exception as e:
        logger.info(f"Fetch appointment by AptNum={apt_num} failed: {str(e)}", extra={'request_id': request_id})
    return None

async def notify_ghl_sync_success(event_id: str, user_id: str, request_id: str):
    async with httpx.AsyncClient() as client:
        url = f"{GOHIGHLEVEL_API_URL}/calendars/appointments/{event_id}/notes"
        try:
            headers = {
                'Authorization': f'Bearer {GOHIGHLEVEL_API_KEY}',
                'Version': "2021-07-28",
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            data = {
                "userId": user_id,
                "body": "Appointment synced in OD"
            }
            response = await client.request("POST", url, headers=headers, json=data, timeout=5)
            response.raise_for_status()
            return response.json() if response.content else {}
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (400, 404):
                logger.info(f"User ID or Event ID invalid (status: {e.response.status_code}, response: {e.response.text})", extra={'request_id': request_id})
            if e.response.status_code == 401:
                logger.error(f"Unauthorized error for {url}: {str(e)}, response: {e.response.text}", extra={'request_id': request_id})
            if e.response.status_code == 429:
                logger.error(f"Rate limit exceeded for {url}: {str(e)}, response: {e.response.text}", extra={'request_id': request_id})
            logger.error(f"HTTP error for {url}: {str(e)}, response: {e.response.text}", extra={'request_id': request_id})
        except httpx.RequestError as e:
            logger.error(f"Request failed for {url}: {str(e)}", extra={'request_id': request_id})

# --- MAIN ROUTE --------------------------------------------------------------

@app.post("/ghl-to-opendental")
async def ghl_to_opendental(request: Request):
    request_id = str(uuid.uuid4())
    try:
        headers_dict = dict(request.headers)
        logger.info(f"Received webhook headers: {json.dumps(headers_dict, indent=2)}", extra={'request_id': request_id})
        data = await request.json()
        logger.info(f"Received webhook raw payload: {json.dumps(data, indent=2)}", extra={'request_id': request_id})
        if not await validate_webhook_secret(request, request_id):
            logger.error("Invalid webhook secret", extra={'request_id': request_id})
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

        normalized_data = {normalize_field_name(k): v for k, v in (data.get('customData', data).items())}
        logger.info(f"Received webhook normalized payload: {json.dumps(normalized_data, indent=2)}", extra={'request_id': request_id})

        # interpret CSC flag
        csc_raw = str(normalized_data.get('csc', '')).strip().lower()
        csc_yes = csc_raw in {'yes', 'y', 'true', '1'}

        # capture popup (optional)
        popup_text = str(normalized_data.get('popup', '') or '').strip()

        required_fields = [
            'appointment_date', 'appointment_time', 'appointment end date time',
            'notes', 'last name', 'calendar', 'contact id', 'date of birth', 'first name',
            'customer_phone', 'gender', 'family_phone'
        ]
        missing_fields = [field for field in required_fields if field not in normalized_data or not normalized_data[field]]
        phones_missing = 'customer_phone' in missing_fields and 'family_phone' in missing_fields

        if 'customer_phone' in missing_fields:
            missing_fields.remove('customer_phone')

        if 'family_phone' in missing_fields:
            missing_fields.remove('family_phone')
            
        if missing_fields or phones_missing:
                if phones_missing:
                    missing_fields.append('phone or family phone')

                reason = f"Missing required fields: {missing_fields}"

                await notify_chat_failure(
                            entity="Webhook validation",
                            last_name=normalized_data.get("last name", ""),
                            first_name=normalized_data.get("first name", ""),
                            birth_date=normalized_data.get("date of birth", ""),
                            appt_start=normalized_data.get("appointment_date", "") + " " + normalized_data.get("appointment_time", ""),
                            appt_end=normalized_data.get("appointment end date time", ""),
                            reason=reason,
                            user=normalized_data.get("users", "User not assigned yet"),
                            request_id=request_id
                        )
                logger.error(reason, extra={'request_id': request_id})
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=reason
                )

        # event id (for Drive mapping)
        event_id = _extract_event_id(normalized_data)
        if not event_id:
            logger.warning("No event id found in payload; flow will still work but cannot use Drive mapping for updates.", extra={'request_id': request_id})

        # date/time parsing
        birth_date = validate_and_parse_date(normalized_data['date of birth'], 'date of birth', request_id)
        if not birth_date:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or missing date of birth format, expected MMM DDth YYYY")

        appt_date = validate_and_parse_date(normalized_data['appointment_date'], 'appointment_date', request_id)
        if not appt_date:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid appointment_date format, expected Month DD, YYYY")

        appt_time = validate_and_parse_time(normalized_data['appointment_time'], normalized_data['appointment_date'], 'appointment_time', request_id)
        if not appt_time:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid appointment_time format, expected HH:MM AM/PM")

        end_time = validate_and_parse_time(normalized_data['appointment end date time'], normalized_data['appointment_date'], 'appointment end date time', request_id)
        if not end_time:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid APPOINTMENT END DATE TIME format, expected HH:MM AM/PM")

        start_time = appt_time
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        if end_dt < start_dt:
            appt_date_dt = datetime.strptime(appt_date, '%Y-%m-%d')
            next_day = appt_date_dt + timedelta(days=1)
            end_time = validate_and_parse_time(normalized_data['appointment end date time'], next_day.strftime('%B %d, %Y'), 'appointment end date time', request_id)
            logger.info(f"Adjusted end_time to next day: {end_time}", extra={'request_id': request_id})
        logger.info(f"Parsed times: start_time={start_time}, end_time={end_time}", extra={'request_id': request_id})

        ghl_status = data.get('calendar', {}).get('appoinmentStatus', '').lower()
        if ghl_status not in STATUS_MAP:
            logger.warning(f"Empty or invalid appointment status: {ghl_status}, defaulting to 'confirmed'", extra={'request_id': request_id})
            ghl_status = 'confirmed'
        if not normalized_data.get('customer_email'):
            logger.warning(f"Empty customer_email for Contact ID: {normalized_data['contact id']}", extra={'request_id': request_id})

        clinic_num = await get_clinic_num(normalized_data['calendar'], request_id)
        if not clinic_num:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"No ClinicNum mapped for calendar: {normalized_data['calendar']}")

        # provider for patient (map or fallback)
        try:
            prov_num_for_patient = CLINIC_TO_PROVNUM_MAP.get(str(clinic_num)) or await get_default_provider(clinic_num, request_id)
            if not prov_num_for_patient:
                logger.warning(f"No provider found for ClinicNum={clinic_num} (will create patient without PriProv)", extra={'request_id': request_id})
        except Exception as _e:
            prov_num_for_patient = None
            logger.warning(f"Could not resolve provider for ClinicNum={clinic_num}: {str(_e)}", extra={'request_id': request_id})

        pattern = calculate_pattern(start_time, end_time, request_id)
        if not pattern:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid appointment duration or time format")

        # Determine appointment type from mapping fields
        ortho_mapping_raw = normalized_data.get('ortho mapping', '')
        gp_mapping_raw = normalized_data.get('gp mapping', '')
        if not (isinstance(ortho_mapping_raw, str) and ortho_mapping_raw.strip()) and not (isinstance(gp_mapping_raw, str) and gp_mapping_raw.strip()):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing mapping: provide either ORTHO MAPPING or GP MAPPING")

        appointment_type_name = None
        if isinstance(ortho_mapping_raw, str) and ortho_mapping_raw.strip():
            ortho_l = ortho_mapping_raw.strip().lower()
            if 'credit/cash' in ortho_l:
                appointment_type_name = 'cash consult'
            elif 'through insurance' in ortho_l:
                appointment_type_name = 'insurance consult'
            else:
                logger.warning(f"ORTHO MAPPING provided but no rule matched: {ortho_mapping_raw}", extra={'request_id': request_id})
        if appointment_type_name is None and isinstance(gp_mapping_raw, str) and gp_mapping_raw.strip():
            gp_l = gp_mapping_raw.strip().lower()
            if 'adult' in gp_l:
                appointment_type_name = 'comp ex'
            elif 'child' in gp_l:
                appointment_type_name = 'comp ex child'
            else:
                logger.warning(f"GP MAPPING provided but no rule matched: {gp_mapping_raw}", extra={'request_id': request_id})

        if appointment_type_name is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid mapping content: ORTHO/GP MAPPING did not match expected values")

        appointment_type_num = APPOINTMENT_TYPE_MAP.get(appointment_type_name)
        if not appointment_type_num:
            logger.warning(f"Derived appointment type name '{appointment_type_name}' has no ID in APPOINTMENT_TYPE_MAP; proceeding without AppointmentTypeNum", extra={'request_id': request_id})

        last_name = normalized_data.get('last name', '').strip()
        first_name = normalized_data.get('first name', '').strip()
        
        language = normalized_data.get('language', '')
        language = convert_language_to_od_format(language)

        if not last_name or not isinstance(last_name, str):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or missing last name")

        pat_num = await get_pat_num_of_patient(request_id, clinic_num, last_name, first_name, birth_date)

        patient_phone = normalized_data.get('customer_phone') or normalized_data.get('family_phone', '')
        
        if not pat_num:
            patient_data = {
                'resourceType': 'Patient',
                'LName': last_name,
                'FName': first_name or 'Unknown',
                'name': [{'family': last_name, 'given': [first_name or 'Unknown']}],
                'BirthDate': birth_date,
                'Gender': GENDER_MAP.get(normalized_data.get('gender', ''), 'Unknown'),
                'PatStatus': ('Patient'),
                'ClinicNum': clinic_num, 
                'WirelessPhone': patient_phone,
                'Language': language
            }

            if \
            str(normalized_data.get('customer_phone', '')).strip() or \
            str(normalized_data.get('family_phone', '')).strip():
                patient_data['TxtMsgOk'] = 'Yes'
            if prov_num_for_patient:
                try:
                    patient_data['PriProv'] = int(prov_num_for_patient)
                except (ValueError, TypeError):
                    patient_data['PriProv'] = prov_num_for_patient
            if normalized_data.get('customer_email'):
                patient_data['telecom'] = [{'system': 'email', 'value': normalized_data['customer_email']}]
                patient_data['Email'] = normalized_data['customer_email']
                patient_data['EmailAddress'] = normalized_data['customer_email']
            try:
                logger.info(f"Creating new patient with data: {json.dumps(patient_data, indent=2)}", extra={'request_id': request_id})
                patient_response = await make_api_request('POST', '/patients', request_id, data=patient_data)
                pat_num = patient_response.get('PatNum')
                if not pat_num:
                    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Patient creation response missing PatNum")
                with cache_lock:
                    patient_cache[patient_key] = pat_num
                logger.info(f"Created new patient PatNum: {pat_num}", extra={'request_id': request_id})
            except Exception as e:
                        reason = str(e)
                        await notify_chat_failure(
                            entity="Patient creation",
                            last_name=last_name,
                            first_name=first_name,
                            birth_date=birth_date,
                            appt_start=start_time,
                            appt_end=end_time,
                            reason=reason,
                            request_id=request_id
                        )
                        logger.error(f"Failed to create patient: {reason}", extra={'request_id': request_id})
                        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Patient creation failed: {reason}")

        if not pat_num:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to find or create patient")

        # Attach referrals
        referred_value = normalized_data.get('referred')
        await _upsert_patient_field_referral(pat_num, referred_value, request_id)

        apt_datetime = start_time
        normalized_calendar = normalize_calendar_name(normalized_data['calendar'])
        op_map = CALENDAR_TO_OP_MAP.get(normalized_calendar, {})
        if not op_map:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"No operatory mapping for calendar: {normalized_calendar}")

        # Determine OD status & provider
        apt_status = STATUS_MAP[ghl_status]
        prov_num = CLINIC_TO_PROVNUM_MAP.get(str(clinic_num)) or await get_default_provider(clinic_num, request_id)
        if not prov_num:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"No provider available for ClinicNum: {clinic_num}")

        # ----------------- NEW FIRST: Drive mapping check ---------------------
        if GOOGLE_DRIVE_FILE_ID and event_id:
            ev_lock = await _get_event_lock(event_id)
            async with ev_lock:
                mapping = await event_map_load(GOOGLE_DRIVE_FILE_ID, request_id)
                stored = mapping.get(event_id)
                if stored and str(stored.get("clinicNum")) == str(clinic_num):
                    # We have an exact mapping → update that specific appointment
                    apt_id = str(stored.get("aptNum"))
                    stored_pat = str(stored.get("patNum"))
                    if stored_pat and str(stored_pat) != str(pat_num):
                        # prefer the stored PatNum for safety
                        pat_num = stored_pat

                    # Try to keep existing Op if present
                    existing = await _fetch_appointment(apt_id, request_id)
                    existing_op = None
                    if existing and existing.get("Op"):
                        existing_op = existing.get("Op")

                    # If cancelling, push to 'cancelled' op, else try to keep or select
                    if apt_status == 'Broken':
                        op_num = op_map.get('cancelled')
                        if not op_num:
                            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"No cancelled operatory for calendar: {normalized_calendar}")
                    elif apt_status == 'Complete':
                        op_num = existing_op or (op_map.get('scheduled') or [None])[0]
                    else:  # Scheduled
                        op_num = existing_op
                        if not op_num:
                            op_num = await get_operatory(normalized_data['calendar'], 'Scheduled', apt_datetime, clinic_num, pat_num, request_id)
                            if not op_num:
                                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"No operatory available for status {ghl_status} at {apt_datetime} in ClinicNum: {clinic_num}")

                    appointment_data = {
                        'resourceType': 'Appointment',
                        'PatNum': pat_num,
                        'AptStatus': apt_status,
                        'AptDateTime': apt_datetime,
                        'Pattern': pattern,
                        'Op': op_num,
                        'ProvNum': prov_num,
                        'participant': [{
                            'actor': {'reference': f"Patient/{pat_num}"},
                            'status': 'accepted'
                        }],
                        'extension': [{
                            'url': 'ClinicNum',
                            'valueString': clinic_num
                        }],
                        'description': f"Synced from GHL (Drive-map) - Status: {ghl_status}, Calendar: {normalized_calendar}, Op: {op_num}, Pattern: {pattern}, Notes: {normalized_data['notes']}, Derived Type: {appointment_type_name}",
                        'Note': normalized_data['notes']
                    }
                    if appointment_type_num:
                        appointment_data['AppointmentTypeNum'] = appointment_type_num

                    await make_api_request('PUT', f'/appointments/{apt_id}', request_id, data=appointment_data)
                    logger.info(f"[Drive-map] Updated appointment AptNum={apt_id} for PatNum={pat_num} (event_id={event_id})", extra={'request_id': request_id})

                    # Update mapping's meta and save
                    stored["lastDate"] = apt_datetime.split("T")[0]
                    stored["updatedAt"] = _now_iso()
                    mapping[event_id] = stored
                    await event_map_save(GOOGLE_DRIVE_FILE_ID, mapping, request_id)

                    if event_id:
                        await notify_ghl_sync_success(event_id, normalized_data.get('user_id', ''), request_id)

                    return JSONResponse(content={'status': 'success', 'mode': 'updated-by-drive-map', 'aptNum': apt_id}, status_code=200)
                # else: fall through to legacy path (create or same-day update)
        # ---------------------------------------------------------------------

        # Legacy idempotency lock per clinic/patient/datetime (your original guard)
        dedupe_key = f"{clinic_num}:{pat_num}:{apt_datetime}"
        logger.info(f"Acquiring idempotency lock for {dedupe_key}", extra={'request_id': request_id})
        lock = await _get_dedupe_lock(dedupe_key)
        async with lock:
            logger.info(f"Idempotency lock acquired for {dedupe_key}", extra={'request_id': request_id})

            # same-day search across mapped columns (original behavior)
            allowed_ops_scheduled = op_map.get('scheduled', [])
            allowed_ops_completed = op_map.get('completed', [])
            allowed_ops_cancelled = [op_map.get('cancelled')] if op_map.get('cancelled') else []
            search_ops_all = (allowed_ops_scheduled or []) + (allowed_ops_completed or []) + allowed_ops_cancelled

            apt_id, existing_op = await _find_same_day_appt_in_ops(
                clinic_num=clinic_num,
                pat_num=str(pat_num),
                apt_datetime=apt_datetime,
                allowed_ops=search_ops_all,
                request_id=request_id
            )
            if apt_status == 'Broken':
                op_num = op_map.get('cancelled')
                if not op_num:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"No cancelled operatory for calendar: {normalized_calendar}")
            elif apt_status == 'Complete':
                op_num = existing_op or await get_operatory(normalized_data['calendar'], 'Scheduled', apt_datetime, clinic_num, pat_num, request_id)
                if not op_num:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"No operatory available for completion at {apt_datetime}")
            else:
                if apt_id and existing_op and str(existing_op) in {str(o) for o in allowed_ops_scheduled}:
                    op_num = existing_op
                else:
                    op_num = await get_operatory(normalized_data['calendar'], 'Scheduled', apt_datetime, clinic_num, pat_num, request_id)
                    if not op_num:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"No operatory available for status {ghl_status} at {apt_datetime} in ClinicNum: {clinic_num}")

            appointment_data = {
                'resourceType': 'Appointment',
                'PatNum': pat_num,
                'AptStatus': apt_status,
                'AptDateTime': apt_datetime,
                'Pattern': pattern,
                'Op': op_num,
                'ProvNum': prov_num,
                'participant': [{
                    'actor': {'reference': f"Patient/{pat_num}"},
                    'status': 'accepted'
                }],
                'extension': [{
                    'url': 'ClinicNum',
                    'valueString': clinic_num
                }],
                'description': f"Synced from GHL - Status: {ghl_status}, Calendar: {normalized_calendar}, Op: {op_num}, Pattern: {pattern}, Notes: {normalized_data['notes']}, Derived Type: {appointment_type_name}",
                'Note': normalized_data['notes']
            }
            if appointment_type_num:
                appointment_data['AppointmentTypeNum'] = appointment_type_num

            if apt_id:
                try:
                    await make_api_request('PUT', f'/appointments/{apt_id}', request_id, data=appointment_data)
                    logger.info(f"Updated appointment {apt_id} for PatNum: {pat_num}", extra={'request_id': request_id})
                except Exception as e:
                    reason = str(e)
                    await notify_chat_failure(
                        entity="Appointment update",
                        last_name=last_name,
                        first_name=first_name,
                        birth_date=birth_date,
                        appt_start=start_time,
                        appt_end=end_time,
                        reason=reason,
                        request_id=request_id
                    )
                    raise

                # Do not create commlog/popup on updates
            else:
                 # --- Idempotency guard ---
                dedupe_hash = hashlib.md5(f"{clinic_num}:{pat_num}:{apt_datetime}".encode()).hexdigest()
                if appointment_cache.get(dedupe_hash) == "creating":
                    logger.warning(f"Duplicate prevention: Appointment already being created for {dedupe_hash}", extra={'request_id': request_id})
                    return JSONResponse(content={'status': 'duplicate-skipped'}, status_code=200)
                appointment_cache[dedupe_hash] = "creating"
                try:
                    create_resp = await make_api_request('POST', '/appointments', request_id, data=appointment_data)
                    created_apt_id = await _get_created_apt_id(create_resp, pat_num, clinic_num, apt_datetime, request_id)
                    logger.info(f"Created new appointment for PatNum: {pat_num}, Contact ID: {normalized_data['contact id']} AptNum: {created_apt_id}", extra={'request_id': request_id})
                except Exception as e:
                    reason = str(e)
                    await notify_chat_failure(
                        entity="Appointment creation",
                        last_name=last_name,
                        first_name=first_name,
                        birth_date=birth_date,
                        appt_start=start_time,
                        appt_end=end_time,
                        reason=reason,
                        request_id=request_id
                    )
                    raise
                finally:
                       appointment_cache.pop(dedupe_hash, None)


                if csc_yes and created_apt_id:
                    await _maybe_add_csc_procedure(
                        pat_num=pat_num,
                        apt_num=str(created_apt_id),
                        clinic_num=str(clinic_num),
                        prov_num=str(prov_num),
                        request_id=request_id,
                        proc_date=apt_datetime.split('T')[0]
                    )

                await _create_commlog(
                    pat_num=pat_num,
                    note=str(normalized_data['notes']),
                    request_id=request_id
                )

                if created_apt_id and popup_text:
                    await _create_popup(
                        pat_num=pat_num,
                        apt_num=created_apt_id,
                        note=popup_text,
                        request_id=request_id
                    )

                # NEW: after creation, persist mapping to Drive (if we have event id)
                if GOOGLE_DRIVE_FILE_ID and event_id and created_apt_id:
                    ev_lock = await _get_event_lock(event_id)
                    async with ev_lock:
                        mapping = await event_map_load(GOOGLE_DRIVE_FILE_ID, request_id)
                        mapping[event_id] = {
                            "aptNum": str(created_apt_id),
                            "patNum": str(pat_num),
                            "clinicNum": str(clinic_num),
                            "lastDate": apt_datetime.split("T")[0],
                            "contactId": str(normalized_data.get('contact id')),
                            "calendar": normalize_calendar_name(normalized_data['calendar']),
                            "updatedAt": _now_iso()
                        }

                        saved = await event_map_save(GOOGLE_DRIVE_FILE_ID, mapping, request_id)
                        if saved:
                            logger.info(f"[Drive-map] Stored mapping for event_id={event_id} → AptNum={created_apt_id}, PatNum={pat_num}", extra={'request_id': request_id})
                        else:
                            logger.error(f"[Drive-map] Failed to save mapping for event_id={event_id}", extra={'request_id': request_id})
            
            if event_id:
                await notify_ghl_sync_success(event_id, normalized_data.get('user_id', ''), request_id)

            return JSONResponse(content={'status': 'success'}, status_code=200)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}", extra={'request_id': request_id})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Internal server error: {str(e)}")

@app.post("/patient-sync")
async def patient_sync(request: Request):
    request_id = str(uuid.uuid4())
    try:
        headers_dict = dict(request.headers)
        logger.info(f"Received patient sync webhook headers: {json.dumps(headers_dict, indent=2)}", extra={'request_id': request_id})
        data = await request.json()
        logger.info(f"Received patient sync webhook raw payload: {json.dumps(data, indent=2)}", extra={'request_id': request_id})
        if not await validate_webhook_secret(request, request_id):
            logger.error("Invalid patient sync webhook secret", extra={'request_id': request_id})
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

        normalized_data = {normalize_field_name(k): v for k, v in (data.get('customData', data).items())}

        language = normalized_data.get('contact_language', '')
        language = convert_language_to_od_format(language)

        if language == "invalid":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Not valid language")
        
        first_name = normalized_data.get('contact_first_name', '')
        last_name = normalized_data.get('contact_last_name', '')

        birth_date = validate_and_parse_date(normalized_data['contact_birth_date'], 'date of birth', request_id)
        if not birth_date:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or missing date of birth format, expected MMM DDth YYYY")
        
        clinic_num = await get_clinic_num(normalized_data['contact_calendar'], request_id)
        
        if not last_name or not isinstance(last_name, str):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or missing last name")

        patient_data = {
            'Language': language,
            'LName': last_name,
            'FName': first_name,
        }

        pat_num = await get_pat_num_of_patient(request_id, clinic_num, last_name, first_name, birth_date)

        if not pat_num:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Patient does not exist in OD Database")

        patient_response = await make_api_request('PUT', '/patients/'+str(pat_num), request_id, data=patient_data)
        pat_num = patient_response.get('PatNum')

        if not pat_num:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Patient creation response missing PatNum")
        
        logger.info(f"Received patient sync webhook normalized payload: {json.dumps(normalized_data, indent=2)}", extra={'request_id': request_id})
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing patient sync webhook: {str(e)}", extra={'request_id': request_id})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Internal server error: {str(e)}")