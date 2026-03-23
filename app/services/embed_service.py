"""
Embed Token Service — Generación de tokens para embedding de Power BI.

WHY: Para embeber un reporte de Power BI en una app web, necesitamos
un "embed token" temporal que autoriza al frontend a renderizar el
reporte sin que el usuario final necesite credenciales de PBI.

MOCK mode: Retorna una configuración simulada con IDs ficticios.
Esto permite al frontend desarrollar la lógica de embedding sin
depender de una licencia PBI Pro.

LIVE mode: Usa MSAL → Power BI REST API → genera un embed token real
que el frontend pasa al Power BI JS SDK para renderizar el reporte.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from app.auth.power_bi_auth import PowerBIAuthManager
from app.core.config import settings

logger = logging.getLogger(__name__)

# WHY: Lazy init. En modo LIVE, MSAL puede requerir resolución DNS
# durante init. Si falla y se hace al importar módulo, tumba TODO el
# backend aunque nadie llame /embed-config. Con lazy init, el fallo
# queda aislado al endpoint de embed.
_auth_manager: PowerBIAuthManager | None = None


def _get_auth_manager() -> PowerBIAuthManager:
    """Obtiene/crea el auth manager de forma lazy."""
    global _auth_manager
    if _auth_manager is None:
        _auth_manager = PowerBIAuthManager()
    return _auth_manager


async def get_embed_config(
    report_id: str,
    tenant_id: str,
) -> dict:
    """
    Genera la configuración completa para embeber un reporte de PBI.

    WHY: El frontend necesita 3 cosas para renderizar un reporte:
    1. embedUrl — URL del reporte en el servicio de PBI
    2. accessToken — Token temporal de autorización
    3. reportId — ID del reporte en Power BI

    En MOCK mode, retornamos valores simulados para que el frontend
    pueda desarrollar la UI sin depender de PBI.

    Args:
        report_id: UUID interno del reporte (de nuestra tabla reports).
        tenant_id: UUID del tenant (seguridad multi-tenant).

    Returns:
        Dict con embedUrl, accessToken, reportId, tokenExpiration, mode.
    """
    if settings.pbi_api_mode == "MOCK":
        return _get_mock_embed_config(report_id)

    return await _get_live_embed_config(report_id, tenant_id)


def _get_mock_embed_config(report_id: str) -> dict:
    """
    Genera configuración simulada para MOCK mode.

    WHY: En modo MOCK, el frontend mostrará un área de demo visual
    en lugar del reporte real. Esta configuración le dice al frontend
    que está en modo simulado.
    """
    expiration = datetime.now(timezone.utc) + timedelta(hours=1)

    config = {
        "mode": "MOCK",
        "reportId": report_id,
        "embedUrl": f"https://app.powerbi.com/reportEmbed?reportId=MOCK-{report_id}",
        "accessToken": "mock-embed-token-for-development",
        "tokenType": "Embed",
        "tokenExpiration": expiration.isoformat(),
        "permissions": "View",
        "message": (
            "Modo MOCK activo. El frontend muestra una demo visual. "
            "Cambia PBI_API_MODE=LIVE para embeber reportes reales."
        ),
    }

    logger.info("🧪 Embed config generada en modo MOCK para reporte %s", report_id)
    return config


async def _get_live_embed_config(
    report_id: str,
    tenant_id: str,
) -> dict:
    """
    Genera embed config real usando la API de Power BI.

    WHY: En modo LIVE, necesitamos:
    1. Obtener un access token de Azure AD (via MSAL)
    2. Llamar a la API de Power BI para generar un embed token
    3. Retornar la config completa al frontend

    El embed token tiene una expiración corta (~1 hora) por seguridad.
    """
    import httpx

    # 1. Obtener access token de Azure AD
    access_token = await _get_auth_manager().acquire_token()

    # 2. Obtener la info del reporte desde nuestra DB
    from app.db.supabase_client import get_supabase_client
    client = get_supabase_client()
    report = (
        client.table("reports")
        .select("pbi_report_id, pbi_workspace_id, pbi_dataset_id")
        .eq("id", report_id)
        .eq("tenant_id", tenant_id)
        .single()
        .execute()
    )

    if not report.data:
        raise ValueError(f"Reporte {report_id} no encontrado para tenant {tenant_id}")

    pbi_report_id = report.data["pbi_report_id"]
    pbi_workspace_id = report.data["pbi_workspace_id"]
    # 3. Generar embed token via Power BI REST API
    async with httpx.AsyncClient() as http:
        # Primero obtener el embed URL del reporte
        report_url = (
            f"https://api.powerbi.com/v1.0/myorg/groups/{pbi_workspace_id}"
            f"/reports/{pbi_report_id}"
        )
        report_resp = await http.get(
            report_url,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        report_resp.raise_for_status()
        report_info = report_resp.json()

        # Generar embed token
        token_url = (
            f"https://api.powerbi.com/v1.0/myorg/groups/{pbi_workspace_id}"
            f"/reports/{pbi_report_id}/GenerateToken"
        )
        token_resp = await http.post(
            token_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json={"accessLevel": "Edit", "allowSaveAs": False},
        )
        token_resp.raise_for_status()
        token_data = token_resp.json()

    config = {
        "mode": "LIVE",
        "reportId": pbi_report_id,
        "embedUrl": report_info.get("embedUrl", ""),
        "accessToken": token_data.get("token", ""),
        "tokenType": "Embed",
        "tokenExpiration": token_data.get("expiration", ""),
        "permissions": "Edit",
    }

    logger.info("🔴 Embed config LIVE generada para reporte %s", pbi_report_id)
    return config
