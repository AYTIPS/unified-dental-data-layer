from billing.toroforge.toroforge_client.client import ToroForgeClient
from billing.toroforge.exceptions import ToroForgeValidationError

class ToroForgeKeyStoreClient:
    def __init__(self, client: ToroForgeClient) -> None:
        self.client = client

    
    async def create_keystore(self, *, password: str) -> str:
        data = await self.client.call_write(
            method="POST",
            path="/keystore",
            op="createkey",
            params=[
                {"name": "pwd", "value": password}
            ]
        )

        address = data.get("address")
        if not address:
            raise ToroForgeValidationError(
                f"ToroForge createkey response missing address: {data}"
            )
        return address
    

    async def verify_key(self, *, address: str, password: str)-> bool:
        data = await self.client.call_write(
            method="POST",
            path="/keystore/",
            op="verifykey",
            params=[
                {"name": "addr", "value": address},
                {"name": "pwd", "value": password},
            ],
        )

        return bool(data.get("result"))
    