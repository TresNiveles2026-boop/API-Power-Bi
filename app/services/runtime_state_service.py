"""
Runtime State Service — Persistencia ligera por tenant_id/report_id.

WHY: Permite recordar capacidades bloqueadas por SDK y onboarding ya mostrado
sin depender del storage del navegador. Esto mantiene la UX consistente entre
sesiones y dispositivos.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from app.db.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)

RUNTIME_STATE_TABLE = "report_runtime_state"


@dataclass(frozen=True)
class RuntimeState:
    tenant_id: str
    report_id: str
    blocked_capabilities: dict[str, bool]
    suggested_measures_shown: list[str]
    user_acknowledged: dict[str, bool]
    persistence_enabled: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "report_id": self.report_id,
            "blocked_capabilities": self.blocked_capabilities,
            "suggested_measures_shown": self.suggested_measures_shown,
            "user_acknowledged": self.user_acknowledged,
            "persistence_enabled": self.persistence_enabled,
        }


def _default_state(tenant_id: str, report_id: str, *, enabled: bool) -> RuntimeState:
    return RuntimeState(
        tenant_id=tenant_id,
        report_id=report_id,
        blocked_capabilities={},
        suggested_measures_shown=[],
        user_acknowledged={},
        persistence_enabled=enabled,
    )


def _is_missing_table_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return ("relation" in msg and RUNTIME_STATE_TABLE in msg) or ("does not exist" in msg and RUNTIME_STATE_TABLE in msg)


def _normalize_row(row: dict[str, Any]) -> RuntimeState:
    return RuntimeState(
        tenant_id=str(row.get("tenant_id", "") or ""),
        report_id=str(row.get("report_id", "") or ""),
        blocked_capabilities=dict(row.get("blocked_capabilities") or {}),
        suggested_measures_shown=list(row.get("suggested_measures_shown") or []),
        user_acknowledged=dict(row.get("user_acknowledged") or {}),
        persistence_enabled=True,
    )


async def get_or_create_runtime_state(tenant_id: str, report_id: str) -> RuntimeState:
    """
    Retorna el estado runtime del reporte.

    NOTE: Si la tabla no existe (entorno sin migración aplicada), devolvemos defaults
    con persistence_enabled=False para que el frontend no dependa de ella.
    """
    client = get_supabase_client()
    try:
        res = (
            client.table(RUNTIME_STATE_TABLE)
            .select("*")
            .eq("tenant_id", tenant_id)
            .eq("report_id", report_id)
            .limit(1)
            .execute()
        )
    except Exception as exc:
        if _is_missing_table_error(exc):
            logger.warning("⚠️ Runtime state table missing (%s). Persistence disabled.", RUNTIME_STATE_TABLE)
            return _default_state(tenant_id, report_id, enabled=False)
        raise

    if res.data:
        return _normalize_row(res.data[0])

    payload = {
        "tenant_id": tenant_id,
        "report_id": report_id,
        "blocked_capabilities": {},
        "suggested_measures_shown": [],
        "user_acknowledged": {},
    }
    try:
        ins = client.table(RUNTIME_STATE_TABLE).insert(payload).execute()
        if ins.data:
            return _normalize_row(ins.data[0])
    except Exception as exc:
        # Race condition: alguien más insertó; re-leer.
        if not _is_missing_table_error(exc):
            logger.debug("Runtime state insert raced: %s", exc)

    # Último intento: leer de nuevo
    res2 = (
        client.table(RUNTIME_STATE_TABLE)
        .select("*")
        .eq("tenant_id", tenant_id)
        .eq("report_id", report_id)
        .limit(1)
        .execute()
    )
    if res2.data:
        return _normalize_row(res2.data[0])
    return _default_state(tenant_id, report_id, enabled=True)


def _merge_bool_map(old: dict[str, bool], new: dict[str, bool]) -> dict[str, bool]:
    out = dict(old or {})
    for k, v in (new or {}).items():
        if not k:
            continue
        out[str(k)] = bool(v)
    return out


def _merge_str_list(old: list[str], new: list[str]) -> list[str]:
    s = {str(x) for x in (old or []) if str(x)}
    for x in (new or []):
        xs = str(x)
        if xs:
            s.add(xs)
    return sorted(s)


async def patch_runtime_state(
    tenant_id: str,
    report_id: str,
    *,
    blocked_capabilities: dict[str, bool] | None = None,
    suggested_measures_shown: list[str] | None = None,
    user_acknowledged: dict[str, bool] | None = None,
    replace: bool = False,
) -> RuntimeState:
    """
    Actualiza el runtime state con merge determinista.

    replace=True reemplaza totalmente los campos provistos; por defecto merge.
    """
    current = await get_or_create_runtime_state(tenant_id, report_id)
    if not current.persistence_enabled:
        # No-op si no hay tabla (evita romper UX).
        return current

    if replace:
        next_blocked = dict(blocked_capabilities or current.blocked_capabilities)
        next_measures = list(suggested_measures_shown or current.suggested_measures_shown)
        next_ack = dict(user_acknowledged or current.user_acknowledged)
    else:
        next_blocked = _merge_bool_map(current.blocked_capabilities, blocked_capabilities or {})
        next_measures = _merge_str_list(current.suggested_measures_shown, suggested_measures_shown or [])
        next_ack = _merge_bool_map(current.user_acknowledged, user_acknowledged or {})

    client = get_supabase_client()
    try:
        res = (
            client.table(RUNTIME_STATE_TABLE)
            .update(
                {
                    "blocked_capabilities": next_blocked,
                    "suggested_measures_shown": next_measures,
                    "user_acknowledged": next_ack,
                }
            )
            .eq("tenant_id", tenant_id)
            .eq("report_id", report_id)
            .execute()
        )
        if res.data:
            return _normalize_row(res.data[0])
    except Exception as exc:
        if _is_missing_table_error(exc):
            logger.warning("⚠️ Runtime state table missing during patch. Persistence disabled.")
            return _default_state(tenant_id, report_id, enabled=False)
        raise

    # Si update no devolvió filas (caso raro), re-leer.
    return await get_or_create_runtime_state(tenant_id, report_id)

