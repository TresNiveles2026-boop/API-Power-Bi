"""
Audit Logger — Records API operations to Supabase for compliance.

WHY: En un sistema multi-tenant de BI, es crítico saber quién hizo
qué y cuándo. El audit log permite detectar abusos, debugging de
producción, y cumplimiento regulatorio (SOC2, GDPR).

Las escrituras son fire-and-forget (no bloquean el request principal).
Si la escritura falla, se loguea el error pero no afecta al usuario.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from app.db.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)


def _safe_uuid(value: str | None) -> str | None:
    """Retorna UUID válido o None para valores no compatibles."""
    if not value:
        return None
    try:
        return str(UUID(value))
    except (ValueError, TypeError):
        return None


async def log_audit_event(
    tenant_id: str,
    endpoint: str,
    method: str,
    status_code: int = 200,
    api_key_id: str | None = None,
    request_summary: dict[str, Any] | None = None,
    ip_address: str | None = None,
) -> None:
    """
    Registra un evento en la tabla audit_log de Supabase.

    WHY: Fire-and-forget — si falla, no bloquea el request.
    Los logs son para compliance y debugging, no para lógica de negocio.
    """
    try:
        safe_api_key_id = _safe_uuid(api_key_id)
        client = get_supabase_client()
        client.table("audit_log").insert({
            "tenant_id": tenant_id,
            "api_key_id": safe_api_key_id,
            "endpoint": endpoint,
            "method": method,
            "status_code": status_code,
            "request_summary": request_summary or {},
            "ip_address": ip_address,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }).execute()

        logger.debug(
            "📝 Audit: %s %s tenant=%s status=%d",
            method,
            endpoint,
            tenant_id,
            status_code,
        )
    except Exception as exc:
        # Non-critical: log the error but don't crash the request
        logger.error("⚠️ Audit log write failed: %s", exc)
