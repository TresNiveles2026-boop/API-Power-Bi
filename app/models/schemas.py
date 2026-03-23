"""
Pydantic Schemas — Modelos de request/response para la API.

WHY: Pydantic garantiza que los datos que entran y salen de la API
son exactamente del tipo esperado. Si alguien envía un JSON con un
campo faltante o de tipo incorrecto, FastAPI retorna un 422 automático
con un mensaje claro, en lugar de un crash en runtime.

DECISIÓN: Separamos los schemas en Create (input), Response (output)
y Update (parcial) siguiendo el patrón DTO (Data Transfer Object).
Esto evita exponer campos internos como id o created_at en los inputs.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# ╔══════════════════════════════════════════════════════════════════╗
# ║                         TENANTS                                ║
# ╚══════════════════════════════════════════════════════════════════╝


class TenantCreate(BaseModel):
    """Input para crear un tenant."""
    name: str = Field(..., min_length=1, max_length=200, examples=["Empresa Demo"])
    slug: str = Field(..., min_length=1, max_length=100, examples=["demo"])
    config: dict[str, Any] = Field(default_factory=dict)


class TenantResponse(BaseModel):
    """Output al consultar un tenant."""
    id: str
    name: str
    slug: str
    config: dict[str, Any]
    is_active: bool
    created_at: datetime


# ╔══════════════════════════════════════════════════════════════════╗
# ║                         REPORTS                                ║
# ╚══════════════════════════════════════════════════════════════════╝


class ReportCreate(BaseModel):
    """Input para registrar un reporte de Power BI."""
    tenant_id: str = Field(..., examples=["uuid-del-tenant"])
    pbi_report_id: str = Field(..., examples=["abc-123-report"])
    pbi_dataset_id: str = Field(..., examples=["def-456-dataset"])
    pbi_workspace_id: str = Field(..., examples=["ghi-789-workspace"])
    name: str = Field(..., min_length=1, max_length=300, examples=["Dashboard de Ventas"])
    description: str = Field(default="", examples=["Reporte principal de ventas Q4"])


class ReportResponse(BaseModel):
    """Output al consultar un reporte."""
    id: str
    tenant_id: str
    pbi_report_id: str
    pbi_dataset_id: str
    pbi_workspace_id: str
    name: str
    description: str
    schema_version: int
    is_active: bool
    created_at: datetime


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    SEMANTIC DICTIONARY                         ║
# ╚══════════════════════════════════════════════════════════════════╝


class ColumnSchema(BaseModel):
    """
    Una columna/medida individual del modelo de Power BI.

    WHY: Este es el "átomo" del diccionario semántico. La IA recibe
    una lista de estos para entender qué columnas existen, sus tipos,
    y si son medidas DAX o columnas regulares.
    """
    table_name: str = Field(..., examples=["Ventas"])
    column_name: str = Field(..., examples=["Monto"])
    data_type: str = Field(..., examples=["Decimal"])
    description: str = Field(default="", examples=["Monto total de la venta en USD"])
    is_measure: bool = Field(default=False)
    dax_expression: str = Field(
        default="",
        examples=["SUM(Ventas[Monto])"],
    )
    sample_values: list[Any] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SyncSchemaRequest(BaseModel):
    """
    Input para POST /sync-schema.

    WHY: Recibe el esquema completo de un reporte de Power BI en una
    sola llamada. El array de columns contiene todas las tablas y
    columnas que la IA necesita conocer para escribir DAX válido.
    """
    report_id: str = Field(..., examples=["uuid-del-reporte"])
    tenant_id: str = Field(..., examples=["uuid-del-tenant"])
    columns: list[ColumnSchema] = Field(
        ...,
        min_length=0,
        examples=[[
            {
                "table_name": "Ventas",
                "column_name": "Monto",
                "data_type": "Decimal",
                "description": "Monto total de la venta",
                "is_measure": False,
            },
        ]],
    )


class SyncSchemaResponse(BaseModel):
    """Output de la sincronización del esquema."""
    status: str
    report_id: str
    columns_synced: int
    message: str


class SyncSchemaPowerBIRequest(BaseModel):
    """Input para sincronizar esquema desde Power BI (REST)."""
    report_id: str = Field(..., examples=["uuid-del-reporte"])
    tenant_id: str = Field(..., examples=["uuid-del-tenant"])
    powerbi_access_token: str | None = Field(
        default=None,
        description=(
            "Access token delegado de Microsoft (scope Power BI). "
            "Si se envía, se usará en lugar del Service Principal."
        ),
    )


class SyncSchemaPowerBIResponse(SyncSchemaResponse):
    """Output de la sincronización desde Power BI."""
    mode: str = Field(
        default="full",
        examples=["full", "operational"],
        description="full=esquema completo (Scanner API); operational=fallback por SDK.",
    )
    admin_blocked: bool = Field(
        default=False,
        description="true si el tenant bloquea /admin APIs y se requiere fallback por SDK.",
    )
    columns: list[ColumnSchema] = Field(default_factory=list)


class SemanticDictionaryResponse(BaseModel):
    """
    Output de GET /schema/{report_id}.

    WHY: Formato estructurado que el prompt de Gemini consume
    directamente. Incluye metadata del reporte + lista completa
    de columnas para que la IA sepa exactamente qué datos existen.
    """
    report_id: str
    report_name: str
    schema_version: int
    tables: dict[str, list[ColumnSchema]]
    total_columns: int


# ╔══════════════════════════════════════════════════════════════════╗
# ║                      API RESPONSES                             ║
# ╚══════════════════════════════════════════════════════════════════╝


class ErrorResponse(BaseModel):
    """
    Respuesta de error amigable.

    WHY: Según las Reglas de Oro del Plan Maestro, la API NUNCA debe
    devolver un 500 críptico. Siempre un mensaje comprensible.
    """
    status: str = "error"
    message: str
    detail: str = ""
