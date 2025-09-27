from pydantic import (
    SecretStr,
)
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)


class Settings(BaseSettings):
    """Application configuration."""

    POSTGRES_DB_HOST_NAME: str
    POSTGRES_DB_NAME: str
    POSTGRES_DB_PORT: int
    POSTGRES_DB_PASSWORD: SecretStr
    POSTGRES_DB_USER_NAME: str

    # Redis configuration
    REDIS_HOST: str = 'localhost'
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: SecretStr | None = None
    REDIS_DB: int = 0

    model_config = SettingsConfigDict(
        env_file='.env',
        extra='forbid',
    )


settings = Settings()  # noqa
