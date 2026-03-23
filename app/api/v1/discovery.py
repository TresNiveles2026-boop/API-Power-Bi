"""
Discovery API v1 — Data Discovery Engine endpoints.
"""

from __future__ import annotations

import io
import logging

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile, status

from app.auth.auth_middleware import CurrentUser, get_current_user, require_tenant_match
from app.auth.rate_limiter import rate_limiter
from app.services.audit import log_audit_event
from app.services.discovery_service import profile_dataframe_and_upsert

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/discovery", tags=["Phase 1 — Discovery Engine"])


@router.post(
    "/profile",
    summary="Perfilar semánticamente un archivo tabular (CSV/Excel)",
)
async def discovery_profile(
    request: Request,
    tenant_id: str = Query(...),
    report_id: str = Query(...),
    table_name: str = Form(...),
    file: UploadFile = File(...),
    user: CurrentUser = Depends(get_current_user),
) -> dict:
    """
    Recibe un archivo tabular, lo perfila con Pandas, enriquece semánticamente con Gemini
    y hace upsert automático en semantic_dictionaries.
    """
    require_tenant_match(user, tenant_id)
    rate_limiter.check(user.tenant_id, "default")

    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Debe adjuntar un archivo CSV o Excel.",
        )

    filename = file.filename.lower()
    raw = await file.read()
    if not raw:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El archivo está vacío.",
        )

    try:
        if filename.endswith(".csv"):
            try:
                df = pd.read_csv(io.BytesIO(raw))
            except UnicodeDecodeError:
                # fallback para CSVs con codificación no UTF-8
                df = pd.read_csv(io.BytesIO(raw), encoding="latin-1")
        elif filename.endswith(".xlsx") or filename.endswith(".xls"):
            df = pd.read_excel(io.BytesIO(raw))
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Formato no soportado. Use .csv, .xlsx o .xls.",
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Error leyendo archivo de discovery: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"No se pudo procesar el archivo tabular: {exc}",
        ) from exc

    if df.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El archivo no contiene filas de datos.",
        )

    try:
        profile = await profile_dataframe_and_upsert(
            tenant_id=tenant_id,
            report_id=report_id,
            table_name=table_name,
            df=df,
        )
    except Exception as exc:
        logger.error("Error ejecutando discovery profile: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No se pudo completar el perfilado semántico.",
        ) from exc

    await log_audit_event(
        tenant_id=user.tenant_id,
        endpoint="/api/v1/discovery/profile",
        method="POST",
        api_key_id=user.api_key_id,
        request_summary={
            "report_id": report_id,
            "table_name": table_name,
            "filename": file.filename,
            "rows": int(df.shape[0]),
            "columns": int(df.shape[1]),
        },
        ip_address=request.client.host if request.client else None,
    )

    return {
        "status": "success",
        "tenant_id": tenant_id,
        "report_id": report_id,
        "table_name": table_name,
        "rows_profiled": int(df.shape[0]),
        "columns_profiled": int(df.shape[1]),
        "profile": profile.model_dump(),
    }

