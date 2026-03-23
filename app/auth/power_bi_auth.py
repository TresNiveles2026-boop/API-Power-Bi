"""
Power BI Authentication — MSAL Service Principal con Token Cache.

WHY: Power BI Embedded usa tokens OAuth2 de Azure AD con expiración ~1h.
Sin un cache proactivo, cada request al frontend requeriría una nueva
llamada a Azure AD, sumando ~300ms de latencia y riesgo de throttling.

Este módulo implementa un cache en memoria que refresca el token 5 minutos
antes de que expire, asegurando que el token siempre esté "caliente" cuando
se necesita. En modo MOCK, retorna un token ficticio sin contactar Azure.

DECISIÓN ARQUITECTÓNICA: Usamos MSAL (Microsoft Authentication Library)
porque es la librería oficial de Microsoft para Service Principals.
Alternativas como requests-oauthlib requieren construir el flujo OAuth
manualmente, lo que es propenso a errores sutiles de seguridad.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from app.core.config import settings

logger = logging.getLogger(__name__)

# Scopes necesarios para Power BI REST API
PBI_SCOPES: list[str] = ["https://analysis.windows.net/powerbi/api/.default"]

# Margen de seguridad: refrescar N segundos antes de la expiración real
TOKEN_REFRESH_MARGIN_SECONDS: int = 300  # 5 minutos


class PowerBIAuthManager:
    """
    Gestor de autenticación para Power BI via Service Principal (Azure AD).

    WHY: Encapsula toda la lógica de auth en una clase dedicada para
    cumplir el Single Responsibility Principle. El orquestador y los
    servicios solo llaman a acquire_token() sin preocuparse por el
    mecanismo de obtención o renovación.

    Soporta dos modos operativos controlados por PBI_API_MODE:
    - MOCK: Retorna token ficticio, sin contactar Azure AD.
    - LIVE: Usa MSAL ConfidentialClientApplication para obtener un
            token real via Client Credentials flow.
    """

    def __init__(self) -> None:
        self._cached_token: str | None = None
        self._token_expiry: float = 0.0
        self._msal_app: Any = None

        if settings.pbi_api_mode == "LIVE":
            self._initialize_msal_app()

    def _initialize_msal_app(self) -> None:
        """
        Inicializa la aplicación MSAL para Client Credentials flow.

        WHY: Creamos la instancia de MSAL una sola vez en __init__
        porque ConfidentialClientApplication mantiene su propio cache
        interno de tokens. Re-crearla en cada llamada perdería ese cache.
        """
        try:
            import msal  # noqa: PLC0415 — Import condicional intencional

            self._msal_app = msal.ConfidentialClientApplication(
                client_id=settings.azure_client_id,
                client_credential=settings.azure_client_secret,
                authority=(
                    f"https://login.microsoftonline.com/{settings.azure_tenant_id}"
                ),
            )
            logger.info(
                "✅ MSAL ConfidentialClientApplication inicializada "
                "para tenant: %s",
                settings.azure_tenant_id,
            )
        except ImportError:
            logger.error(
                "❌ La librería 'msal' no está instalada. "
                "Ejecuta: pip install msal"
            )
            raise

    def _is_token_valid(self) -> bool:
        """
        Verifica si el token cacheado aún es válido.

        WHY: Comparamos contra time.time() + margen de seguridad.
        Si el token expira en los próximos 5 minutos, lo consideramos
        inválido para evitar que expire durante una operación en curso.
        """
        if self._cached_token is None:
            return False
        return time.time() < (self._token_expiry - TOKEN_REFRESH_MARGIN_SECONDS)

    async def acquire_token(self) -> str:
        """
        Obtiene un access_token válido para la Power BI REST API.

        En modo MOCK: retorna un token ficticio inmediatamente.
        En modo LIVE: usa el cache si es válido, o solicita uno nuevo
        a Azure AD via MSAL Client Credentials flow.

        Returns:
            Access token como string.

        Raises:
            RuntimeError: Si MSAL no puede obtener el token (credenciales
                          inválidas, Service Principal sin permisos, etc.)
        """
        # ── Modo MOCK: sin llamadas a Azure ──────────────────────────
        if settings.pbi_api_mode == "MOCK":
            mock_token = "mock-access-token-pbi-development-only"
            logger.debug("🧪 MOCK token retornado (no se contactó Azure AD)")
            return mock_token

        # ── Modo LIVE: verificar cache primero ───────────────────────
        if self._is_token_valid():
            logger.debug(
                "🔑 Token cacheado válido (expira en %.0f segundos)",
                self._token_expiry - time.time(),
            )
            return self._cached_token  # type: ignore[return-value]

        # ── Cache miss o expirado: solicitar nuevo token ─────────────
        logger.info("🔄 Solicitando nuevo token a Azure AD...")
        result = self._msal_app.acquire_token_for_client(scopes=PBI_SCOPES)

        if "access_token" in result:
            self._cached_token = result["access_token"]
            # MSAL retorna expires_in en segundos desde ahora
            self._token_expiry = time.time() + result.get("expires_in", 3600)
            logger.info(
                "✅ Token obtenido exitosamente (expira en %d segundos)",
                result.get("expires_in", 3600),
            )
            return self._cached_token

        # ── Error de autenticación ───────────────────────────────────
        error_description = result.get(
            "error_description",
            "Error desconocido al obtener token de Azure AD.",
        )
        logger.error("❌ Fallo de autenticación MSAL: %s", error_description)
        raise RuntimeError(
            f"No se pudo obtener el token de Power BI: {error_description}"
        )

    def invalidate_cache(self) -> None:
        """
        Fuerza la invalidación del token cacheado.

        WHY: Útil cuando se detecta un 401 Unauthorized en una operación,
        indicando que el token fue revocado antes de su expiración natural.
        """
        self._cached_token = None
        self._token_expiry = 0.0
        logger.info("🗑️ Cache de token invalidado manualmente")
