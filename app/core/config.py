"""
Configuración Central — Pydantic Settings con Feature Flags.

WHY: Usamos pydantic-settings en lugar de cargar os.getenv() manualmente
porque proporciona tipado estricto, validación automática y valores por
defecto. Si alguien pone un valor inválido en .env (ej: PBI_API_MODE=BANANAS),
el sistema falla inmediatamente al arrancar, no en runtime cuando ya es tarde.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuración tipada de la aplicación, cargada desde .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── Feature Flag Principal ──────────────────────────────────────
    pbi_api_mode: Literal["MOCK", "LIVE"] = "MOCK"

    # ── Azure AD / Power BI Service Principal ───────────────────────
    azure_tenant_id: str = ""
    azure_client_id: str = ""
    azure_client_secret: str = ""
    pbi_workspace_id: str = ""
    pbi_report_id: str = ""

    # ── Supabase ────────────────────────────────────────────────────
    supabase_url: str = ""
    supabase_anon_key: str = ""
    supabase_service_role_key: str = ""

    # ── Google AI (Gemini) ──────────────────────────────────────────
    google_ai_api_key: str = ""
    gemini_model: str = "gemini-3-flash-preview"
    gemini_max_retries: int = 2

    # ── App Settings ────────────────────────────────────────────────
    app_env: Literal["development", "staging", "production"] = "development"
    app_debug: bool = True
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    # ── Security (Phase 5) ─────────────────────────────────────────
    api_key_required: bool = False  # Set True in production
    rate_limit_enabled: bool = True
    dev_tenant_id: str = "9d36ff08-691e-4f7d-b1bf-049abf374860"
    cors_origins: str = ""  # Comma-separated: "http://localhost:3002,https://app.example.com"

    # ── Timeouts (UX / Cloud Run) ──────────────────────────────────
    # WHY: El frontend (Vercel) suele tener timeouts agresivos; si el orquestador
    # tarda demasiado, devolvemos una respuesta controlada en lugar de colgar la request.
    # Default aligned with WEB-PROMBI client timeout (120s). Keep a small buffer.
    chat_http_timeout_seconds: int = 110

    # ── Validators ──────────────────────────────────────────────────

    @field_validator("pbi_api_mode", mode="before")
    @classmethod
    def normalize_pbi_mode(cls, value: str) -> str:
        """WHY: Normaliza a mayúsculas para evitar errores por 'mock' vs 'MOCK'."""
        return value.upper().strip()

    @model_validator(mode="after")
    def validate_live_credentials(self) -> "Settings":
        """
        WHY: Si estamos en modo LIVE, las credenciales de Azure son obligatorias.
        Esto previene que alguien cambie a LIVE sin configurar los secretos,
        causando errores crípticos en runtime.
        """
        if self.pbi_api_mode == "LIVE":
            missing: list[str] = []
            if not self.azure_tenant_id:
                missing.append("AZURE_TENANT_ID")
            if not self.azure_client_id:
                missing.append("AZURE_CLIENT_ID")
            if not self.azure_client_secret:
                missing.append("AZURE_CLIENT_SECRET")
            if missing:
                raise ValueError(
                    f"PBI_API_MODE=LIVE requiere las siguientes variables: "
                    f"{', '.join(missing)}. Configúralas en .env o cambia a MOCK."
                )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    WHY: Singleton via lru_cache. Evita re-leer el .env en cada request.
    Se crea una sola instancia al primer acceso y se reutiliza.
    """
    return Settings()


# Atajo de acceso directo para imports simples:
#   from app.core.config import settings
settings = get_settings()
