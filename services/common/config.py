import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Core flags
    offline: bool = True
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

    # Models & device
    hf_home: str | None = None
    embedding_model: str = "intfloat/e5-base"
    mnli_model: str = "roberta-base-mnli"
    force_device: str | None = None


settings = Settings()

# Back-compat / env mappings
_offline_mode = os.getenv("OFFLINE_MODE")
if _offline_mode is not None:
    settings.offline = _offline_mode.strip().lower() in ("1", "true", "yes", "on")

_hf = os.getenv("HF_HOME")
if _hf:
    settings.hf_home = _hf

_em = os.getenv("EMBEDDING_MODEL")
if _em:
    settings.embedding_model = _em

_mnli = os.getenv("MNLI_MODEL")
if _mnli:
    settings.mnli_model = _mnli

_fd = os.getenv("FORCE_DEVICE")
if _fd:
    settings.force_device = _fd
