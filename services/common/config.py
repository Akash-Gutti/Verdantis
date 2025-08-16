from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    offline: bool = True
    mode: str = "cpu-first"
    bus_backend: str = "file"
    redis_url: str = "redis://localhost:6379/0"
    log_level: str = "INFO"

    ingest_port: int = 8001
    rag_verify_port: int = 8002
    vision_port: int = 8003
    causal_port: int = 8004
    zk_port: int = 8005
    policy_port: int = 8006
    alerts_port: int = 8007

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
