import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Accept unknown env keys; read from .env (case-insensitive)
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Core flags
    offline: bool = True  # preferred field
    mode: str = "cpu-first"
    bus_backend: str = "file"
    redis_url: str = "redis://localhost:6379/0"
    log_level: str = "INFO"

    # Ports
    ingest_port: int = 8001
    rag_verify_port: int = 8002
    vision_port: int = 8003
    causal_port: int = 8004
    zk_port: int = 8005
    policy_port: int = 8006
    alerts_port: int = 8007

    # Optional extras (avoid crashes if present in .env)
    hf_home: str | None = None


settings = Settings()

# Back-compat / mapping from existing .env keys
# OFFLINE_MODE -> offline
_offline_mode = os.getenv("OFFLINE_MODE")
if _offline_mode is not None:
    settings.offline = _offline_mode.strip().lower() in ("1", "true", "yes", "on")

# HF_HOME -> hf_home
_hf = os.getenv("HF_HOME")
if _hf:
    settings.hf_home = _hf
