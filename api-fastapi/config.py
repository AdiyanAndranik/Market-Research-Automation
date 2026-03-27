from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    app_name: str = "AI Market Research Agent"
    app_version: str = "1.0.0"
    debug: bool = False

    database_url: str

    openai_api_key: str
    openai_model: str = "gpt-4o-mini"
    openai_max_tokens: int = 1500

    scraper_api_key: str = ""
    scraper_api_url: str = "http://api.scraperapi.com"

    secret_key: str

    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""

    slack_webhook_url: str = ""

    max_products_per_source: int = 20
    request_timeout: int = 30
    request_delay: float = 1.5

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()