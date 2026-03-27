"""
API Routes v1 — Endpoints de Fase 1-5.

WHY: Separamos las rutas en un router dedicado (APIRouter) en lugar
de ponerlas directamente en main.py. Esto permite:
1. Versionado de API (v1, v2) sin romper endpoints existentes.
2. Prefijos automáticos (/api/v1/...) sin repetir en cada ruta.
3. Agrupación lógica en la documentación Swagger.

Phase 5: All endpoints now require API key auth (when enabled),
enforce tenant isolation, and log audit events.
"""

from __future__ import annotations

import logging
import asyncio
import io
import json
import zipfile
from typing import Any

import httpx
from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile, status

from app.auth.auth_middleware import CurrentUser, get_current_user, require_tenant_match
from app.auth.rate_limiter import rate_limiter
from app.db.supabase_client import get_supabase_client
from app.models.schemas import (
    ColumnSchema,
    EmbedConfigRequest,
    ErrorResponse,
    MeasureTemplateListResponse,
    ReportCreate,
    ReportResponse,
    SemanticDictionaryResponse,
    SyncSchemaRequest,
    SyncSchemaResponse,
    SyncSchemaPowerBIRequest,
    SyncSchemaPowerBIResponse,
    TenantCreate,
    TenantResponse,
)
from app.ai.gemini_client import GeminiExhaustedError, GeminiTimeoutError
from app.ai.models import ChatRequest, ChatResponse, ExplainRequest, VisualAction
from app.services.audit import log_audit_event
from app.services.chat_history_service import (
    get_conversation,
    get_conversation_messages,
    get_conversations,
    update_conversation_title,
)
from app.services.embed_service import get_embed_config
from app.services.orchestrator_service import process_chat_message
from app.services.explain_service import generate_visual_explanation
from app.services.dataset_service import process_uploaded_file
from app.services.semantic_service import (
    format_dictionary_for_prompt,
    get_semantic_dictionary,
    save_uploaded_schema,
    sync_schema,
)
from app.services.measure_template_service import get_measure_templates
from app.services.pbi_schema_sync_service import (
    AdminSchemaBlockedError,
    SchemaReadBlockedError,
    sync_schema_from_powerbi,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Phase 1 — Semantic Layer"])


# ╔══════════════════════════════════════════════════════════════════╗
# ║                         TENANTS                                ║
# ╚══════════════════════════════════════════════════════════════════╝


@router.get(
    "/measure-templates",
    response_model=MeasureTemplateListResponse,
    status_code=status.HTTP_200_OK,
)
async def list_measure_templates(_: CurrentUser = Depends(get_current_user)) -> MeasureTemplateListResponse:
    return MeasureTemplateListResponse(templates=get_measure_templates())


@router.post(
    "/tenants",
    response_model=TenantResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Crear un nuevo tenant",
    responses={409: {"model": ErrorResponse}},
)
async def create_tenant(
    payload: TenantCreate,
    request: Request,
    user: CurrentUser = Depends(get_current_user),
) -> TenantResponse:
    """
    Registra una nueva empresa/cliente en el sistema.

    WHY: El tenant es la unidad base de multi-tenancy. Todo reporte,
    schema y conversación está vinculado a un tenant_id.
    """
    rate_limiter.check(user.tenant_id, "default")
    client = get_supabase_client()

    try:
        result = (
            client.table("tenants")
            .insert({
                "name": payload.name,
                "slug": payload.slug,
                "config": payload.config,
            })
            .execute()
        )
    except Exception as exc:
        logger.error("Error creando tenant: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"El slug '{payload.slug}' ya existe o hubo un error: {exc}",
        ) from exc

    data = result.data[0]
    logger.info("🏢 Tenant creado: %s (%s)", data["name"], data["id"])

    await log_audit_event(
        tenant_id=user.tenant_id,
        endpoint="/api/v1/tenants",
        method="POST",
        api_key_id=user.api_key_id,
        request_summary={"tenant_name": payload.name},
        ip_address=request.client.host if request.client else None,
    )

    return TenantResponse(**data)


@router.get(
    "/tenants",
    response_model=list[TenantResponse],
    summary="Listar todos los tenants",
)
async def list_tenants(
    user: CurrentUser = Depends(get_current_user),
) -> list[TenantResponse]:
    """Lista todos los tenants activos del sistema."""
    rate_limiter.check(user.tenant_id, "default")
    client = get_supabase_client()
    result = (
        client.table("tenants")
        .select("*")
        .eq("is_active", True)
        .order("created_at", desc=True)
        .execute()
    )
    return [TenantResponse(**row) for row in result.data]


# ╔══════════════════════════════════════════════════════════════════╗
# ║                         REPORTS                                ║
# ╚══════════════════════════════════════════════════════════════════╝


@router.post(
    "/reports",
    response_model=ReportResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Registrar un reporte de Power BI",
    responses={409: {"model": ErrorResponse}},
)
async def create_report(
    payload: ReportCreate,
    request: Request,
    user: CurrentUser = Depends(get_current_user),
) -> ReportResponse:
    """
    Registra un reporte de Power BI en el sistema.

    WHY: Este endpoint vincula un reporte PBI con un tenant.
    Phase 5: Enforces tenant isolation — you can only create
    reports for your own tenant.
    """
    require_tenant_match(user, payload.tenant_id)
    rate_limiter.check(user.tenant_id, "default")
    client = get_supabase_client()

    try:
        result = (
            client.table("reports")
            .insert({
                "tenant_id": payload.tenant_id,
                "pbi_report_id": payload.pbi_report_id,
                "pbi_dataset_id": payload.pbi_dataset_id,
                "pbi_workspace_id": payload.pbi_workspace_id,
                "name": payload.name,
                "description": payload.description,
            })
            .execute()
        )
    except Exception as exc:
        logger.error("Error creando reporte: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Reporte ya registrado o error: {exc}",
        ) from exc

    data = result.data[0]
    logger.info("📊 Reporte registrado: %s (%s)", data["name"], data["id"])

    await log_audit_event(
        tenant_id=user.tenant_id,
        endpoint="/api/v1/reports",
        method="POST",
        api_key_id=user.api_key_id,
        request_summary={"report_name": payload.name},
        ip_address=request.client.host if request.client else None,
    )

    return ReportResponse(**data)


@router.get(
    "/reports/{tenant_id}",
    response_model=list[ReportResponse],
    summary="Listar reportes de un tenant",
)
async def list_reports(
    tenant_id: str,
    user: CurrentUser = Depends(get_current_user),
) -> list[ReportResponse]:
    """Lista todos los reportes activos de un tenant específico."""
    require_tenant_match(user, tenant_id)
    rate_limiter.check(user.tenant_id, "default")
    client = get_supabase_client()
    result = (
        client.table("reports")
        .select("*")
        .eq("tenant_id", tenant_id)
        .eq("is_active", True)
        .order("created_at", desc=True)
        .execute()
    )
    return [ReportResponse(**row) for row in result.data]


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    SCHEMA SYNC & RETRIEVAL                     ║
# ╚══════════════════════════════════════════════════════════════════╝


@router.post(
    "/sync-schema",
    response_model=SyncSchemaResponse,
    summary="Sincronizar el esquema de Power BI",
    responses={404: {"model": ErrorResponse}},
)
async def sync_schema_endpoint(
    payload: SyncSchemaRequest,
    request: Request,
    user: CurrentUser = Depends(get_current_user),
) -> SyncSchemaResponse:
    """
    Recibe el JSON con tablas/columnas de Power BI y lo guarda en Supabase.

    Phase 5: Enforces tenant isolation — API key must match the tenant_id.
    """
    require_tenant_match(user, payload.tenant_id)
    rate_limiter.check(user.tenant_id, "default")

    # Verificar que el reporte existe y pertenece al tenant
    client = get_supabase_client()
    report_check = (
        client.table("reports")
        .select("id")
        .eq("id", payload.report_id)
        .eq("tenant_id", payload.tenant_id)
        .execute()
    )

    if not report_check.data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Reporte '{payload.report_id}' no encontrado para "
                f"el tenant '{payload.tenant_id}'. Créalo primero con "
                f"POST /api/v1/reports."
            ),
        )

    if not payload.columns:
        # Reporte en blanco o escaneo operacional sin campos.
        # No tocamos Supabase para evitar borrar/ensuciar el esquema previo.
        return SyncSchemaResponse(
            status="success",
            report_id=payload.report_id,
            columns_synced=0,
            message="⚠️ Sin columnas para sincronizar (reporte en blanco o sin campos en uso).",
        )

    synced_count = await sync_schema(
        report_id=payload.report_id,
        tenant_id=payload.tenant_id,
        columns=payload.columns,
    )

    await log_audit_event(
        tenant_id=user.tenant_id,
        endpoint="/api/v1/sync-schema",
        method="POST",
        api_key_id=user.api_key_id,
        request_summary={"columns_synced": synced_count},
        ip_address=request.client.host if request.client else None,
    )

    return SyncSchemaResponse(
        status="success",
        report_id=payload.report_id,
        columns_synced=synced_count,
        message=f"✅ {synced_count} columnas sincronizadas exitosamente",
    )


@router.post(
    "/sync-schema-powerbi",
    response_model=SyncSchemaPowerBIResponse,
    summary="Sincronizar esquema desde Power BI (REST)",
    responses={404: {"model": ErrorResponse}},
)
async def sync_schema_powerbi_endpoint(
    payload: SyncSchemaPowerBIRequest,
    request: Request,
    user: CurrentUser = Depends(get_current_user),
) -> SyncSchemaPowerBIResponse:
    """
    Obtiene el esquema real desde Power BI y lo guarda en Supabase.
    """
    require_tenant_match(user, payload.tenant_id)
    rate_limiter.check(user.tenant_id, "default")

    try:
        synced_count, columns = await sync_schema_from_powerbi(
            report_id=payload.report_id,
            tenant_id=payload.tenant_id,
            powerbi_access_token=payload.powerbi_access_token,
        )
    except (AdminSchemaBlockedError, SchemaReadBlockedError):
        await log_audit_event(
            tenant_id=user.tenant_id,
            endpoint="/api/v1/sync-schema-powerbi",
            method="POST",
            api_key_id=user.api_key_id,
            request_summary={"columns_synced": 0, "mode": "operational", "admin_blocked": True},
            ip_address=request.client.host if request.client else None,
        )
        return SyncSchemaPowerBIResponse(
            status="success",
            report_id=payload.report_id,
            columns_synced=0,
            message="Requiere fallback de SDK",
            mode="operational",
            admin_blocked=True,
            columns=[],
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        logger.error("❌ Error sincronizando schema Power BI: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No se pudo sincronizar el esquema desde Power BI.",
        ) from exc

    await log_audit_event(
        tenant_id=user.tenant_id,
        endpoint="/api/v1/sync-schema-powerbi",
        method="POST",
        api_key_id=user.api_key_id,
        request_summary={"columns_synced": synced_count},
        ip_address=request.client.host if request.client else None,
    )

    return SyncSchemaPowerBIResponse(
        status="success",
        report_id=payload.report_id,
        columns_synced=synced_count,
        message=f"✅ {synced_count} columnas sincronizadas exitosamente",
        mode="full",
        admin_blocked=False,
        columns=columns,
    )


@router.get(
    "/schema/{report_id}",
    response_model=SemanticDictionaryResponse,
    summary="Obtener el diccionario semántico de un reporte",
    responses={404: {"model": ErrorResponse}},
)
async def get_schema(
    report_id: str,
    tenant_id: str,
    user: CurrentUser = Depends(get_current_user),
) -> SemanticDictionaryResponse:
    """Recupera el diccionario semántico completo de un reporte."""
    require_tenant_match(user, tenant_id)
    rate_limiter.check(user.tenant_id, "default")

    dictionary = await get_semantic_dictionary(
        report_id=report_id,
        tenant_id=tenant_id,
    )

    if dictionary is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No se encontró diccionario semántico para el reporte "
                f"'{report_id}'. ¿Ya ejecutaste POST /api/v1/sync-schema?"
            ),
        )

    return dictionary


@router.get(
    "/schema/{report_id}/prompt",
    response_model=dict[str, str],
    summary="Obtener el diccionario formateado para prompt de IA",
)
async def get_schema_for_prompt(
    report_id: str,
    tenant_id: str,
    user: CurrentUser = Depends(get_current_user),
) -> dict[str, str]:
    """Retorna el diccionario en formato texto para el System Prompt."""
    require_tenant_match(user, tenant_id)
    rate_limiter.check(user.tenant_id, "default")

    dictionary = await get_semantic_dictionary(
        report_id=report_id,
        tenant_id=tenant_id,
    )

    if dictionary is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Diccionario no encontrado para reporte '{report_id}'.",
        )

    prompt_text = format_dictionary_for_prompt(dictionary)
    return {"prompt_context": prompt_text}


# ╔══════════════════════════════════════════════════════════════════╗
# ║                     CHAT (PHASE 2)                             ║
# ╚══════════════════════════════════════════════════════════════════╝


@router.post(
    "/chat",
    response_model=ChatResponse,
    summary="Interactuar con el orquestador de BI",
    tags=["Phase 2 — AI Orchestrator"],
    responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
async def chat(
    payload: ChatRequest,
    request: Request,
    user: CurrentUser = Depends(get_current_user),
) -> ChatResponse:
    """
    Endpoint principal del orquestador.

    Phase 5: Rate limited (30/min), authenticated, audited.
    """
    require_tenant_match(user, payload.tenant_id)
    rate_limiter.check(user.tenant_id, "chat")

    try:
        # WHY: No imponemos un timeout HTTP "duro" aquí.
        # El orquestador ya tiene timeout interno y el frontend maneja timeouts del cliente.
        response = await process_chat_message(
            message=payload.message,
            report_id=payload.report_id,
            tenant_id=payload.tenant_id,
            conversation_id=payload.conversation_id,
            visual_context=payload.visual_context,
        )
    except asyncio.CancelledError:
        # Client disconnected / request aborted: no es un error del backend.
        logger.info("🔌 /chat cancelado por el cliente.")
        raise
    except Exception as exc:
        # Degradación controlada: nunca romper el chat por errores del pipeline de IA.
        from app.ai.gemini_client import GeminiExhaustedError, GeminiTimeoutError

        if isinstance(exc, (GeminiExhaustedError, GeminiTimeoutError)):
            action = VisualAction(
                operation="ERROR",
                visualType=None,
                title="",
                explanation=(
                    "La IA tardó demasiado o no estuvo disponible temporalmente. "
                    "Intenta de nuevo en unos segundos."
                ),
                follow_up_questions=["¿Quieres que lo intente de nuevo?"],
            )
            return ChatResponse(
                status="success",
                action=action,
                actions=[action],
                intent="ERROR",
                confidence=0.0,
                retries_used=3,
                conversation_id=payload.conversation_id,
            )

        # ── PARACHUTE (Safety Net de ruta): ──
        # Si algo escapa del orquestador, lo capturamos aquí como WARNING
        # para que Cloud Run NO dispare alertas por falsos positivos.
        logger.warning(
            "⚠️ Error no crítico en /chat (prompt: '%.50s...'): %s",
            payload.message,
            exc,
        )
        action = VisualAction(
            operation="ERROR",
            explanation=(
                "Ocurrió un problema procesando tu solicitud. "
                "¿Podrías reformular tu pregunta de forma más específica?"
            ),
            follow_up_questions=[
                "¿Qué datos te gustaría visualizar?",
                "¿Quieres que lo intente de nuevo?",
            ],
        )
        return ChatResponse(
            status="success",
            action=action,
            actions=[action],
            intent="ERROR",
            confidence=0.0,
            retries_used=0,
            conversation_id=payload.conversation_id,
        )

    await log_audit_event(
        tenant_id=user.tenant_id,
        endpoint="/api/v1/chat",
        method="POST",
        api_key_id=user.api_key_id,
        request_summary={
            "message_preview": payload.message[:50],
            "intent": response.intent,
        },
        ip_address=request.client.host if request.client else None,
    )

    return response


@router.post(
    "/upload-dataset",
    response_model=dict[str, Any],
    summary="Procesar archivo CSV/Excel y devolver su esquema inicial",
    tags=["Phase 5 — Dataset Ingestion"],
)
async def upload_dataset(
    file: UploadFile = File(...),
    report_id: str = Form(...),
    target_table_name: str = Form(..., description="Nombre exacto de la tabla en Power BI"),
    user: CurrentUser = Depends(get_current_user),
) -> dict[str, Any]:
    """Procesa un archivo subido y devuelve tablas, columnas, tipos y muestra."""
    rate_limiter.check(user.tenant_id, "default")

    try:
        file_content = await file.read()
        result = await process_uploaded_file(file_content=file_content, filename=file.filename or "")
        for table in result.get("tables", []):
            if isinstance(table, dict):
                table["table_name"] = target_table_name
        await save_uploaded_schema(
            tenant_id=user.tenant_id,
            report_id=report_id,
            tables_info=result["tables"],
        )
        return {
            "status": "success",
            "report_id": report_id,
            "target_table_name": target_table_name,
            **result,
        }
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        logger.error("❌ Error procesando upload-dataset: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No se pudo procesar el archivo subido.",
        ) from exc


@router.post(
    "/upload-pbit",
    response_model=dict[str, Any],
    summary="Subir plantilla Power BI (.pbit) para extraer esquema (sin datos)",
    tags=["Phase 7 — Safe Template Upload"],
)
async def upload_pbit(
    request: Request,
    file: UploadFile = File(...),
    report_id: str = Form(...),
    tenant_id: str = Form(...),
    user: CurrentUser = Depends(get_current_user),
) -> dict[str, Any]:
    """
    Valida extensión .pbit y extrae el esquema determinísticamente desde DataModelSchema.
    """
    require_tenant_match(user, tenant_id)
    rate_limiter.check(user.tenant_id, "default")

    filename = (file.filename or "").strip()
    lowered = filename.lower()
    if lowered.endswith(".pbix"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "No podemos procesar archivos .pbix. Por favor, sube una Plantilla .pbit "
                "exportada desde Power BI Desktop."
            ),
        )
    if not lowered.endswith(".pbit"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Archivo inválido. Solo se permite extensión .pbit.",
        )

    file_bytes = await file.read()

    # ── Parseo determinista PBIX/PBIT (ZIP en memoria) ───────────────
    try:
        zf = zipfile.ZipFile(io.BytesIO(file_bytes))
    except zipfile.BadZipFile as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Archivo inválido o corrupto. Asegúrate de subir un .pbit válido.",
        ) from exc

    schema_name: str | None = None
    for name in zf.namelist():
        if name == "DataModelSchema" or name.endswith("/DataModelSchema"):
            schema_name = name
            break

    if not schema_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "No se pudo encontrar el esquema del modelo dentro del archivo. "
                "Intenta con otro .pbit o genera el archivo nuevamente."
            ),
        )

    raw_schema = zf.read(schema_name)
    text: str | None = None
    for enc in ("utf-16-le", "utf-8-sig", "utf-8"):
        try:
            text = raw_schema.decode(enc)
            break
        except UnicodeDecodeError:
            continue

    if not text:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No se pudo decodificar el esquema del modelo (DataModelSchema).",
        )

    try:
        schema_json = json.loads(text)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El esquema del modelo no es un JSON válido (DataModelSchema).",
        ) from exc

    model = schema_json.get("model") if isinstance(schema_json, dict) else None
    tables = (model or {}).get("tables") if isinstance(model, dict) else None
    if not isinstance(tables, list):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El archivo no contiene un modelo tabular válido (model.tables).",
        )

    def _map_dtype(dt: Any) -> str:
        if dt is None:
            return "Texto"
        s = str(dt).strip().lower()
        if s in {"int64", "int32", "int16", "double", "decimal", "numeric", "currency", "number"}:
            return "Numérico"
        if s in {"datetime", "date", "datetimezone", "time"}:
            return "Fecha"
        if s in {"boolean", "bool"}:
            return "Booleano"
        if s in {"string", "text"}:
            return "Texto"
        return "Texto"

    extracted: list[Any] = []
    for t in tables:
        if not isinstance(t, dict):
            continue
        tname = str(t.get("name") or "").strip()
        if not tname:
            continue
        if (
            tname.startswith("DateTableTemplate")
            or tname.startswith("LocalDateTable")
            or tname.startswith("DateTemplate")
        ):
            continue

        cols = t.get("columns")
        if isinstance(cols, list):
            for c in cols:
                if not isinstance(c, dict):
                    continue
                cname = str(c.get("name") or "").strip()
                if not cname:
                    continue
                extracted.append(
                    ColumnSchema(
                        table_name=tname,
                        column_name=cname,
                        data_type=_map_dtype(c.get("dataType")),
                        description="",
                        is_measure=False,
                        dax_expression="",
                        sample_values=[],
                        metadata={"source": "pbit_upload", "kind": "column"},
                    )
                )

        measures = t.get("measures")
        if isinstance(measures, list):
            for m in measures:
                if not isinstance(m, dict):
                    continue
                mname = str(m.get("name") or "").strip()
                if not mname:
                    continue
                expr = str(m.get("expression") or "").strip()
                extracted.append(
                    ColumnSchema(
                        table_name=tname,
                        column_name=mname,
                        data_type="Measure",
                        description="",
                        is_measure=True,
                        dax_expression=expr,
                        sample_values=[],
                        metadata={"source": "pbit_upload", "kind": "measure"},
                    )
                )

    if not extracted:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No se encontraron tablas/columnas dentro del modelo del archivo.",
        )

    synced_count = await sync_schema(
        report_id=report_id,
        tenant_id=tenant_id,
        columns=extracted,
    )

    await log_audit_event(
        tenant_id=user.tenant_id,
        endpoint="/api/v1/upload-pbit",
        method="POST",
        api_key_id=user.api_key_id,
        request_summary={"report_id": report_id, "filename": filename},
        ip_address=request.client.host if request and request.client else None,
    )

    return {
        "status": "success",
        "report_id": report_id,
        "tenant_id": tenant_id,
        "filename": filename,
        "columns_synced": synced_count,
        "message": f"✅ Esquema listo: {synced_count} columnas sincronizadas.",
    }


@router.post(
    "/explain",
    response_model=dict[str, str],
    summary="Generar explicación analítica híbrida (Pandas + Gemini)",
    tags=["Phase 3 — Explain Hybrid"],
)
async def explain_visual(
    payload: ExplainRequest,
    request: Request,
    user: CurrentUser = Depends(get_current_user),
) -> dict[str, str]:
    """
    Explica un visual usando hechos deterministas calculados por Pandas.
    """
    rate_limiter.check(user.tenant_id, "chat")

    if not payload.raw_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El campo 'raw_data' es obligatorio y no puede estar vacío.",
        )

    try:
        explanation = await generate_visual_explanation(payload)
    except Exception as exc:
        logger.error("❌ Error en /api/v1/explain: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No se pudo generar la explicación analítica.",
        ) from exc

    await log_audit_event(
        tenant_id=user.tenant_id,
        endpoint="/api/v1/explain",
        method="POST",
        api_key_id=user.api_key_id,
        request_summary={
            "visual_title": payload.visual_title or payload.visual_name,
            "visual_type": payload.visual_type,
            "rows": len(payload.raw_data),
        },
        ip_address=request.client.host if request.client else None,
    )

    return {"status": "success", "explanation": explanation}


# ╔══════════════════════════════════════════════════════════════════╗
# ║                   EMBED TOKENS (PHASE 3)                       ║
# ╚══════════════════════════════════════════════════════════════════╝


@router.post(
    "/embed-config",
    summary="Obtener configuración de embed para Power BI",
    tags=["Phase 3 — Power BI Embed"],
    responses={404: {"model": ErrorResponse}},
)
async def embed_config(
    payload: EmbedConfigRequest,
    request: Request,
    user: CurrentUser = Depends(get_current_user),
) -> dict:
    """
    Retorna la configuración necesaria para embeber un reporte PBI.

    Phase 5: Rate limited (10/min), authenticated.
    """
    require_tenant_match(user, payload.tenant_id)
    rate_limiter.check(user.tenant_id, "embed")

    try:
        config = await get_embed_config(
            report_id=payload.report_id,
            tenant_id=payload.tenant_id,
        )

        await log_audit_event(
            tenant_id=user.tenant_id,
            endpoint="/api/v1/embed-config",
            method="POST",
            api_key_id=user.api_key_id,
            request_summary={"report_id": payload.report_id},
            ip_address=request.client.host if request.client else None,
        )

        return config
    except Exception as exc:
        logger.error("❌ Error generando embed config: %s", exc)
        if isinstance(exc, ValueError):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No se encontró el reporte solicitado para este tenant.",
            ) from exc
        if isinstance(exc, httpx.HTTPStatusError):
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=(
                    "Power BI no respondió correctamente al generar el embed token. "
                    "Verifica permisos del Service Principal y configuración del reporte."
                ),
            ) from exc
        if isinstance(exc, RuntimeError):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    "No se pudo autenticar contra Power BI en este momento. "
                    "Intenta nuevamente en unos minutos."
                ),
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno generando la configuración de embed.",
        ) from exc


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    HISTORY ROUTES (Phase 6)                      ║
# ╚══════════════════════════════════════════════════════════════════╝


@router.get("/conversations", response_model=list[dict[str, Any]])
async def list_conversations(
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
) -> list[dict[str, Any]]:
    """Listar conversaciones recientes del tenant."""
    # 1. Rate Limit
    rate_limiter.check(current_user.tenant_id, "default")

    # 2. Obtener conversaciones
    conversations = await get_conversations(
        tenant_id=current_user.tenant_id,
        limit=50,
    )
    return conversations


@router.get("/conversations/{conversation_id}/messages", response_model=list[dict[str, Any]])
async def list_messages(
    conversation_id: str,
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
) -> list[dict[str, Any]]:
    """Obtener historial de mensajes de una conversación."""
    # 1. Rate Limit
    rate_limiter.check(current_user.tenant_id, "default")

    # 2. Verificar ownership de la conversación
    conversation = await get_conversation(conversation_id)
    if not conversation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversación no encontrada.",
        )
    if conversation["tenant_id"] != current_user.tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No tienes acceso a esta conversación.",
        )

    # 3. Obtener mensajes
    messages = await get_conversation_messages(conversation_id)
    return messages


@router.patch("/conversations/{conversation_id}")
async def update_conversation(
    conversation_id: str,
    payload: dict[str, str],
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
) -> dict[str, str]:
    """Actualizar título de la conversación."""
    title = payload.get("title")
    if not title:
        raise HTTPException(status_code=400, detail="Title required")

    conversation = await get_conversation(conversation_id)
    if not conversation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversación no encontrada.",
        )
    if conversation["tenant_id"] != current_user.tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No tienes acceso a esta conversación.",
        )

    updated = await update_conversation_title(
        conversation_id=conversation_id,
        tenant_id=current_user.tenant_id,
        title=title,
    )
    if not updated:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversación no encontrada para este tenant.",
        )
    return {"status": "success"}
