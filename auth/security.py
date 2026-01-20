from cryptography.fernet import Fernet
from config import settings

ENCRYPTION_KEY = settings.encryption_key
fernet = Fernet(ENCRYPTION_KEY.encode() if isinstance(ENCRYPTION_KEY, str) else ENCRYPTION_KEY)

def encrypt_secret(value : str ):
    if value is None:
        return None
    token = fernet.encrypt(value.encode("utf-8"))
    return token.decode("utf-8")


def decode_secret(value : str ):
    if value is None:
        return None
    token_byte = value.encode("utf-8")
    value_byte = fernet.decrypt(token_byte)
    return value_byte.decode ("utf-8")