from __future__ import annotations 
import logging
from typing import Any 
from billing.toroforge.toroforge_client.kyc_client import ToroForgeKYCClient
from billing.toroforge.exceptions import ToroForgeValidationError
from core.models import Wallet
from uuid import UUID
from sqlalchemy.orm import Session, load_only

logger = logging.getLogger(__name__)


class ToroForgeKYCService:
    def __init__(self, db: Session, kyc_client: ToroForgeKYCClient) -> None:
         self.db = db
         self.kyc_client = kyc_client


    async  def submit_kyc(
         self,
         *,
         first_name: str,
         middle_name: str | None,
         last_name: str,
         bvn: str,
         currency: str,
         phone_number: str,
         dob: str,
         address: str,
    ) -> dict[str, Any]:
         
         
        log_ctx = {
            "currency": currency.strip().upper(),
            "bvn_suffix": self._mask_bvn(bvn),
            "phone_suffix": self._mask_phone(phone_number),
        }
         
        logger.info("ToroForge KYC submission requested", extra=log_ctx)

        response = await self.kyc_client.check_kyc(
            first_name=first_name,
            middle_name=middle_name,
            last_name=last_name,
            bvn=bvn,
            currency=currency,
            phone_number=phone_number,
            dob=dob,
            address=address
        )

        logger.info(
            "ToroForge KYC submission completed",
            extra={
                **log_ctx,
                "result": response.get("result"),
                "message": response.get("message"),
            },
        )

        return response

    async def submit_wallet_kyc(
        self,
        *,
        wallet_id: UUID,
        first_name: str,
        middle_name: str | None,
        last_name: str,
        bvn: str,
        currency: str,
        phone_number: str,
        dob: str,
        address: str,
    ) -> dict[str, Any]:
        wallet = (
            self.db.query(Wallet)
            .options(
                load_only(
                    Wallet.id,
                    Wallet.external_wallet_address,
                )
            )
            .filter(Wallet.id == wallet_id)
            .first()
        )
        if not wallet:
            raise ToroForgeValidationError("Wallet not found")
        if not wallet.external_wallet_address:
            raise ToroForgeValidationError("Wallet has no external address")

        return await self.submit_kyc(
            first_name=first_name,
            middle_name=middle_name,
            last_name=last_name,
            bvn=bvn,
            currency=currency,
            phone_number=phone_number,
            dob=dob,
            address=address,
        )
    
    async def get_address_verification_status(
            self,
            *,
            address: str
    ) -> dict[str, Any]:
        
        log_ctx = {
            "address_suffix": self._mask_address(address),
        }


        logger.info("ToroForge address verification requested", extra=log_ctx)


        response = await self.kyc_client.check_address_verified(address=address)


        logger.info(
            "ToroForge address verification completed",
            extra={
                **log_ctx,
                "verified": response["verified"],
                "provider": response.get("provider"),
            },
        )

        return response
    

    async def check_wallet_kyc_status(self, *, wallet_id: UUID) -> dict[str, Any]:
        wallet = (
            self.db.query(Wallet)
            .options(
                load_only(
                    Wallet.id,
                    Wallet.external_wallet_address,
                )
            )
            .filter(Wallet.id == wallet_id)
            .first()
        )
        if not wallet:
            raise ToroForgeValidationError("Wallet not found")
        if not wallet.external_wallet_address:
            raise ToroForgeValidationError("Wallet has no external address")

        return await self.get_address_verification_status(
            address=wallet.external_wallet_address
        )



    def _mask_bvn(self, bvn:str) -> str:
        normalized = bvn.strip()
        if len(normalized) <= 4:
            return normalized

        return normalized[-4:]


    def _mask_phone(self, phone_number: str) -> str:
        normalized = phone_number.strip()
        if len(normalized) <= 4:
            return normalized
        return normalized[-4:]


    def _mask_address(self, address: str) -> str:
        normalized = address.strip()
        if len(normalized) <= 8:
            return normalized
        return normalized[-8:]



        




        

