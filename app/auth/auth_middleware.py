"""
Auth Middleware — API Key Authentication for all endpoints.

WHY: En las fases anteriores, todos los endpoints eran públicos.
Ahora protegemos la API con API keys vinculadas a un tenant.
Cada request debe incluir el header X-API-Key, que se valida
contra la tabla api_keys en Supabase (almacenadas como SHA-256).

El middleware inyecta un objeto CurrentUser con el tenant_id
verificado, eliminando la necesidad de confiar en el tenant_id
que envía el cliente — el sistema lo infiere del API key.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import APIKeyHeader

from app.core.config import settings
from app.db.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)

# ── Header scheme ─────────────────────────────────────────────
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


@dataclass(frozen=True)
class CurrentUser:
    """
    Contexto de autenticación inyectado en cada request.

    WHY: En lugar de confiar en el tenant_id que el cliente envía,
    lo derivamos del API key validado. Esto previene que un tenant
    acceda a los datos de otro simplemente cambiando el UUID.
    """

    tenant_id: str
    api_key_id: str
    key_name: str


def _hash_key(raw_key: str) -> str:
    """SHA-256 hash del API key para comparar con la DB."""
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


async def get_current_user(
    request: Request,
    api_key: str | None = Depends(api_key_header),
) -> CurrentUser:
    """
    Dependency de FastAPI que valida el API key y retorna el usuario.

    WHY: Como Depends(), se ejecuta ANTES del handler de cada endpoint.
    Si el key es inválido, retorna 401 sin ejecutar la lógica del endpoint.

    En modo desarrollo con API_KEY_REQUIRED=false, permite requests
    sin API key usando el tenant de demo.
    """
    # ── Bypass en desarrollo si la flag está desactivada ──────────
    if not settings.api_key_required:
        logger.debug("🔓 API key validation bypassed (API_KEY_REQUIRED=false)")
        return CurrentUser(
            tenant_id=settings.dev_tenant_id or "dev-tenant",
            api_key_id="dev-bypass",
            key_name="dev-bypass",
        )

    # ── Validar presencia del header ─────────────────────────────
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key missing. Include header: X-API-Key: <your-key>",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    # ── Buscar el hash en Supabase ───────────────────────────────
    key_hash = _hash_key(api_key)
    client = get_supabase_client()

    try:
        result = (
            client.table("api_keys")
            .select("id, tenant_id, name, is_active")
            .eq("key_hash", key_hash)
            .limit(1)
            .execute()
        )
    except Exception as exc:
        logger.error("❌ Error querying api_keys table: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication service error",
        ) from exc

    if not result.data:
        logger.warning("🚫 Invalid API key attempt: %s...", api_key[:8])
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    key_data = result.data[0]

    # ── Verificar que el key esté activo ─────────────────────────
    if not key_data.get("is_active", False):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="API key is deactivated. Contact your administrator.",
        )

    # ── Actualizar last_used_at (fire-and-forget) ────────────────
    try:
        client.table("api_keys").update(
            {"last_used_at": datetime.now(timezone.utc).isoformat()}
        ).eq("id", key_data["id"]).execute()
    except Exception:
        pass  # Non-critical, don't block the request

    user = CurrentUser(
        tenant_id=key_data["tenant_id"],
        api_key_id=key_data["id"],
        key_name=key_data.get("name", "unknown"),
    )

    logger.debug(
        "🔑 Authenticated: tenant=%s key=%s",
        user.tenant_id,
        user.key_name,
    )
    return user


def require_tenant_match(user: CurrentUser, request_tenant_id: str) -> None:
    """
    Verifica que el tenant del API key coincida con el tenant del request.

    WHY: Un tenant no debe poder acceder a los datos de otro.
    Aunque el API key está vinculado a un tenant, el request podría
    enviar un tenant_id diferente. Esta función lo previene.
    """
    if user.tenant_id != request_tenant_id:
        logger.warning(
            "🚫 Tenant mismatch: key=%s requested=%s",
            user.tenant_id,
            request_tenant_id,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied: API key does not match the requested tenant.",
        )
