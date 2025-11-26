from pydantic_settings import BaseSettings
from pydantic import Field 

class Settings (BaseSettings):
    database_username: str = Field(..., env=("database_username", "DATABASE_USERNAME"))  # type: ignore
    database_password: str = Field(..., env=("database_password", "DATABASE_PASSWORD"))   # type: ignore
    database_hostname: str = Field(..., env = ("database_hostname", "DATABASE_HOSTNAME" )) # type: ignore
    database_portname: str = Field(..., env = ("database_portname", "DATABASE_PORTNAME" ))  # type: ignore
    database_name :    str = Field(..., env = ("database_nam", "DATABASE_NAM")) # type: ignore



    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()  # type: ignore 














