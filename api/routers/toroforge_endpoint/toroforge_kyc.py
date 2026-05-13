from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import NoReturn
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session, load_only

from auth.oauth2 import get_current_user
from billing.toroforge.exceptions import (
    ToroForgeAuthError,
    ToroForgeHTTPError,
    ToroForgeTimeoutError,
    ToroForgeUnavailableError,
    ToroForgeValidationError,
)
from billing.toroforge.toroforge_client.client import ToroForgeClient
from billing.toroforge.toroforge_client.kyc_client import ToroForgeKYCClient
from billing.toroforge.toroforge_config import get_toroforge_config
from billing.toroforge.toroforge_service.kyc_service import ToroForgeKYCService
from core.database import get_db
from core.models import Users, Wallet
from core.schemas import (
    toroforge_kyc_submit_request,
    toroforge_kyc_submit_response,
    toroforge_wallet_kyc_status_response,
)
from infra.rbac import require_clinic_access, require_clinic_manage, require_dso_access, require_dso_manage

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/toroforge",
    tags=["ToroForge KYC"],
)


async def get_toroforge_kyc_service(
    db: Session = Depends(get_db),
) -> AsyncIterator[ToroForgeKYCService]:
    config = get_toroforge_config()
    base_client = ToroForgeClient(
        config=config,
        breaker=ToroForgeClient.toroforge_breaker,
    )

    try:
        yield ToroForgeKYCService(
            db=db,
            kyc_client=ToroForgeKYCClient(base_client),
        )
    finally:
        await base_client.aclose()


def raise_kyc_http_error(exc: Exception) -> NoReturn:
    message = str(exc).strip()

    if isinstance(exc, ToroForgeValidationError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=message or "Invalid ToroForge KYC request",
        ) from exc

    if isinstance(exc, (ToroForgeTimeoutError, ToroForgeUnavailableError, ToroForgeHTTPError, ToroForgeAuthError)):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="ToroForge KYC is temporarily unavailable",
        ) from exc

    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Unexpected error while processing ToroForge KYC",
    ) from exc


def get_wallet_for_access_check(
    *,
    db: Session,
    wallet_id: UUID,
) -> Wallet:
    wallet = (
        db.query(Wallet)
        .options(
            load_only(
                Wallet.id,
                Wallet.clinic_id,
                Wallet.dso_id,
            )
        )
        .filter(Wallet.id == wallet_id)
        .first()
    )

    if not wallet:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Wallet not found",
        )

    if wallet.clinic_id is None and wallet.dso_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Wallet is not linked to a clinic or DSO",
        )

    return wallet


def require_wallet_manage_access(
    *,
    db: Session,
    user_id: UUID,
    wallet: Wallet,
) -> None:


    if wallet.clinic_id is not None and wallet.dso_id is not None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="KYC for clinics under a DSO must be completed at the DSO level",
        )
    

    if wallet.clinic_id is not None:
        require_clinic_manage(db=db, user_id=user_id, clinic_id=wallet.clinic_id)
        return

    if wallet.dso_id is not None:
        require_dso_manage(db=db, user_id=user_id, dso_id=wallet.dso_id)
        return

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Wallet is not linked to a clinic or DSO",
    )


def require_wallet_view_access(
    *,
    db: Session,
    user_id: UUID,
    wallet: Wallet,
) -> None:
    if wallet.clinic_id is not None:
        require_clinic_access(db=db, user_id=user_id, clinic_id=wallet.clinic_id)
        return

    if wallet.dso_id is not None:
        require_dso_access(db=db, user_id=user_id, dso_id=wallet.dso_id)
        return

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Wallet is not linked to a clinic or DSO",
    )


@router.post(
    "/wallets/{wallet_id}/kyc",
    status_code=status.HTTP_200_OK,
    response_model=toroforge_kyc_submit_response,
)
async def submit_wallet_kyc(
    wallet_id: UUID,
    payload: toroforge_kyc_submit_request,
    request: Request,
    current_user: Users = Depends(get_current_user),
    db: Session = Depends(get_db),
    kyc_service: ToroForgeKYCService = Depends(get_toroforge_kyc_service),
):
    wallet = get_wallet_for_access_check(db=db, wallet_id=wallet_id)
    require_wallet_manage_access(db=db, user_id=current_user.id, wallet=wallet)

    log_ctx = {
        "request_id": getattr(request.state, "request_id", None),
        "user_id": str(current_user.id),
        "wallet_id": str(wallet_id),
        "clinic_id": str(wallet.clinic_id) if wallet.clinic_id else None,
        "dso_id": str(wallet.dso_id) if wallet.dso_id else None,
    }

    logger.info("ToroForge wallet KYC submission requested", extra=log_ctx)

    try:
        response = await kyc_service.submit_wallet_kyc(
            wallet_id=wallet_id,
            first_name=payload.first_name,
            middle_name=payload.middle_name,
            last_name=payload.last_name,
            bvn=payload.bvn,
            currency=payload.currency,
            phone_number=payload.phone_number,
            dob=payload.dob,
            address=payload.address,
        )
    except Exception as exc:
        logger.exception("ToroForge wallet KYC submission failed", extra=log_ctx)
        raise_kyc_http_error(exc)

    logger.info("ToroForge wallet KYC submission completed", extra=log_ctx)

    return {
        "wallet_id": wallet_id,
        "result": bool(response.get("result", True)),
        "message": response.get("message"),
        "provider_response": response,
    }


@router.get(
    "/wallets/{wallet_id}/kyc-status",
    status_code=status.HTTP_200_OK,
    response_model=toroforge_wallet_kyc_status_response,
)
async def get_wallet_kyc_status(
    wallet_id: UUID,
    request: Request,
    current_user: Users = Depends(get_current_user),
    db: Session = Depends(get_db),
    kyc_service: ToroForgeKYCService = Depends(get_toroforge_kyc_service),
):
    wallet = get_wallet_for_access_check(db=db, wallet_id=wallet_id)
    require_wallet_view_access(db=db, user_id=current_user.id, wallet=wallet)

    log_ctx = {
        "request_id": getattr(request.state, "request_id", None),
        "user_id": str(current_user.id),
        "wallet_id": str(wallet_id),
        "clinic_id": str(wallet.clinic_id) if wallet.clinic_id else None,
        "dso_id": str(wallet.dso_id) if wallet.dso_id else None,
    }

    logger.info("ToroForge wallet KYC status requested", extra=log_ctx)

    try:
        response = await kyc_service.check_wallet_kyc_status(wallet_id=wallet_id)
    except Exception as exc:
        logger.exception("ToroForge wallet KYC status failed", extra=log_ctx)
        raise_kyc_http_error(exc)

    logger.info("ToroForge wallet KYC status completed", extra=log_ctx)

    return {
        "wallet_id": wallet_id,
        "verified": response["verified"],
        "provider": response.get("provider"),
    }
