"""
AI Output Models — Pydantic schemas para las salidas estructuradas de Gemini.

WHY: Definimos los schemas de salida con Pydantic para que Gemini
genere JSON que se parsea automáticamente con validación de tipos.
Si Gemini genera un campo con tipo incorrecto o faltante, Pydantic
captura el error inmediatamente en lugar de propagarlo al frontend.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    ROUTER OUTPUT                               ║
# ╚══════════════════════════════════════════════════════════════════╝


class IntentClassification(BaseModel):
    """Resultado de la clasificación de intención por el Router."""
    intent: str = Field(
        ...,
        description="Tipo de intención: CREATE_VISUAL, UPDATE_VISUAL, FILTER, NAVIGATE, EXPLAIN, UNKNOWN",
    )
    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Nivel de confianza de la clasificación",
    )
    reasoning: str = Field(
        default="",
        description="Razonamiento de la clasificación",
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    GENERATOR OUTPUTS                           ║
# ╚══════════════════════════════════════════════════════════════════╝


class FilterConfig(BaseModel):
    """Configuración de un filtro individual."""
    table: str
    column: str
    operator: str = "In"
    values: list[Any] = Field(default_factory=list)


class SuggestedVisual(BaseModel):
    """Sugerencia de visual para el modo EXPLAIN."""
    description: str
    visualType: str  # noqa: N815 — Mantiene camelCase para compatibilidad con PBI SDK


class DataRoleBinding(BaseModel):
    """Binding enriquecido para data roles (contrato moderno)."""
    table: str | None = None
    column: str | None = None
    ref: str | None = None
    measure: str | None = None
    aggregation: str | None = None
    is_measure: bool | None = None
    data_type: str | None = None

    @model_validator(mode="before")
    @classmethod
    def coerce_legacy_shapes(cls, data: Any) -> Any:
        """
        Acepta variantes comunes del LLM sin romper el grafo:
        - "Tabla[Col]" -> {"ref": "..."}
        - {"table": "...", "column": "...", "measure": true} -> {"table": "...", "column": "...", "is_measure": true}
        """
        if isinstance(data, str):
            value = data.strip()
            return {"ref": value} if value else data

        if not isinstance(data, dict):
            return data

        payload = dict(data)

        # Gemini a veces envía `measure: true|false` en vez de un nombre de medida.
        m = payload.get("measure")
        if isinstance(m, bool):
            payload["is_measure"] = m
            payload.pop("measure", None)
        elif isinstance(m, str) and m.strip():
            # Si se provee un nombre de medida, marcamos como medida.
            payload["measure"] = m.strip()
            payload["is_measure"] = True

        col = payload.get("column")
        if isinstance(col, str):
            cleaned = col.strip()
            # Normaliza brackets sueltos: "Col]" / "[Col]" / "[Col"
            if cleaned.startswith("[") and cleaned.endswith("]"):
                cleaned = cleaned[1:-1].strip()
            elif cleaned.endswith("]") and "[" not in cleaned:
                cleaned = cleaned[:-1].strip()
            elif cleaned.startswith("[") and "]" not in cleaned:
                cleaned = cleaned[1:].strip()
            payload["column"] = cleaned

        ref = payload.get("ref")
        if isinstance(ref, str):
            payload["ref"] = ref.strip()

        tbl = payload.get("table")
        if isinstance(tbl, str):
            payload["table"] = tbl.strip()

        return payload


class TopNConfig(BaseModel):
    """Configuración para filtro TopN nativo del SDK de Power BI.

    Se usa en vez de DAX RANKX/TOPN para filtrar los N elementos
    principales/inferiores de una dimensión, ordenados por una medida.
    """
    count: int = Field(..., ge=1, le=100, description="Cantidad de elementos a mostrar (ej. 5)")
    order_by_column: str = Field(..., description="Columna por la que se ordena (ej. 'Stock disponible')")
    order_by_table: str = Field(default="", description="Tabla de la columna de orden")
    category_column: str = Field(default="", description="Columna de categoría sobre la que se filtra")
    category_table: str = Field(default="", description="Tabla de la columna de categoría")
    direction: str = Field(default="Top", description="'Top' o 'Bottom'")


class VisualLayout(BaseModel):
    """Layout abstracto del visual en el canvas."""
    x: float | None = None
    y: float | None = None
    width: float | None = None
    height: float | None = None


class VisualFormatting(BaseModel):
    """Opciones de formato alto nivel para el visual."""
    title: str | None = None
    showLegend: bool | None = None  # noqa: N815
    showDataLabels: bool | None = None  # noqa: N815
    theme: str | None = None  # Legacy (retrocompatibilidad)
    titleText: str | None = None  # noqa: N815  # Legacy (retrocompatibilidad)


class SemanticColumnProfile(BaseModel):
    """Perfil semántico enriquecido de una columna."""
    name: str
    description: str
    synonyms: list[str] = Field(default_factory=list)
    default_aggregation: str | None = None


class SemanticTableProfile(BaseModel):
    """Perfil semántico enriquecido de una tabla completa."""
    table_name: str
    columns: list[SemanticColumnProfile] = Field(default_factory=list)


VisualOperation = Literal[
    "CREATE",
    "UPDATE",
    "EXPLAIN",
    "DELETE",
    "ERROR",
    # Legacy operations (retrocompatibilidad)
    "CREATE_VISUAL",
    "FILTER",
    "NAVIGATE",
    "UNKNOWN",
]

VisualTypeLiteral = Literal[
    "barChart",
    "columnChart",
    "lineChart",
    "pieChart",
    "donutChart",
    "card",
    "table",
    "matrix",
    "gauge",
    "areaChart",
    "scatterChart",
]

ErrorCode = Literal[
    "SCHEMA_VALIDATION_FAILED",
    "TARGET_AMBIGUOUS",
    "TARGET_NOT_FOUND",
    "TARGET_MISSING",
    "SEMANTIC_FIELD_NOT_FOUND",
    "FILTER_TYPE_MISMATCH",
    "TIME_INTELLIGENCE_REQUIRES_DATE_TABLE",
    "MULTI_FILTER_INTERSECTION_NOTICE",
]


KpiOperation = Literal[
    "distinct_count",
    "percent_of_total",
    "running_total",
    "rank",
    "yoy",
]


class KpiRequirements(BaseModel):
    """
    Requisitos deterministas para ejecutar un KPI (especialmente en cards).

    WHY: Algunas operaciones (DistinctCount en card, YoY, % del total, etc.) pueden
    requerir una medida en el modelo por restricciones del SDK/tenant. En esos casos
    el frontend debe activar el asistente de medidas y evitar intentos inútiles.
    """
    needs_measure: bool = Field(default=False, description="Si true, requiere una medida en el modelo.")
    operation: KpiOperation | None = Field(default=None, description="Operación KPI canónica.")
    measure_template_id: str | None = Field(default=None, description="ID de plantilla determinista (registry).")
    suggested_measure_name: str | None = Field(default=None, description="Nombre sugerido para la medida.")
    table: str | None = Field(default=None, description="Tabla destino (si aplica).")
    column: str | None = Field(default=None, description="Columna destino (si aplica).")
    dax_suggestion: str | None = Field(default=None, description="DAX sugerido ya renderizado (si aplica).")


class VisualAction(BaseModel):
    """
    Acción generada por el orquestador para el frontend.

    WHY: Esta es la estructura central del sistema — el contrato
    entre backend y frontend. El frontend es un ejecutor determinista:
    lee esta estructura y ejecuta la operación correspondiente
    (createVisual, setFilters, etc.) sin tomar decisiones propias.
    """
    operation: VisualOperation = Field(
        ...,
        description=(
            "CREATE | UPDATE | EXPLAIN | DELETE | ERROR "
            "(legacy: CREATE_VISUAL | FILTER | NAVIGATE | UNKNOWN)"
        ),
    )
    visualType: VisualTypeLiteral | None = Field(  # noqa: N815
        default=None,
        description="Tipo de visual soportado por el sistema: barChart, columnChart, lineChart, pieChart, donutChart, card, table, matrix, areaChart, scatterChart.",
    )
    title: str | None = Field(default=None, description="Título del visual")
    dataRoles: dict[str, str | DataRoleBinding | list[Any]] | None = Field(  # noqa: N815
        default=None,
        description="Mapeo de roles de datos: Category, Y, Series",
    )
    dax: str | None = Field(
        default=None,
        description="Código DAX si se necesita crear una medida nueva",
    )
    dax_name: str | None = Field(
        default=None,
        description="Nombre de la medida DAX nueva",
    )
    targetVisualName: str | None = None  # noqa: N815
    layout: VisualLayout | None = None
    layout_intent: str | None = Field(
        default=None,
        description="Intención abstracta de layout: kpi_top | chart_half | chart_full",
    )
    format: VisualFormatting | None = None
    filters: list[FilterConfig] | None = Field(default=None)
    target_page: str | None = Field(default=None, description="Página destino para NAVIGATE")
    explanation: str | None = Field(default=None, description="Explicación amigable en español")
    suggested_visuals: list[SuggestedVisual] | None = Field(default=None)
    follow_up_questions: list[str] | None = Field(default=None)
    error_code: ErrorCode | None = Field(
        default=None,
        description="Código canónico de error para degradación controlada (opcional).",
    )
    query_type: str | None = Field(default=None, description="Operación especial reservada para el backend (ej. TIME_SNAPSHOT_COMPARISON)")
    payload: dict[str, Any] | None = Field(default=None, description="Payload de parámetros para macros del backend")
    top_n: TopNConfig | None = Field(default=None, description="Configuración de filtro TopN nativo (reemplaza DAX RANKX/TOPN)")
    requirements: KpiRequirements | None = Field(default=None, description="Requisitos deterministas para ejecutar el KPI (si aplica).")

    @model_validator(mode="after")
    def validate_by_operation(self) -> "VisualAction":
        """Valida campos requeridos según la operación."""
        operation = str(self.operation or "").upper().strip()

        # Canonización base para evitar fallos de parseo por null explícito del LLM.
        self.title = (self.title or "").strip()
        self.dataRoles = self.dataRoles or {}
        self.dax = (self.dax or "").strip()
        self.dax_name = (self.dax_name or "").strip()
        self.filters = self.filters or []
        self.target_page = (self.target_page or "").strip()
        self.layout_intent = (self.layout_intent or "").strip()
        self.explanation = (self.explanation or "").strip()
        self.suggested_visuals = self.suggested_visuals or []
        self.follow_up_questions = self.follow_up_questions or []

        visual_type = (self.visualType or "").strip()
        target_visual = (self.targetVisualName or "").strip()

        if operation in {"CREATE", "CREATE_VISUAL"} and not visual_type:
            raise ValueError("visualType es obligatorio para operaciones CREATE/CREATE_VISUAL.")

        if operation in {"UPDATE", "EXPLAIN", "DELETE"} and not target_visual:
            raise ValueError(
                "targetVisualName es obligatorio para operaciones UPDATE/EXPLAIN/DELETE."
            )

        return self


class AIResponse(BaseModel):
    """Contenedor maestro para respuestas multi-acción."""
    actions: list[VisualAction] = Field(default_factory=list)


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    VALIDATOR OUTPUT                            ║
# ╚══════════════════════════════════════════════════════════════════╝


class ValidationResult(BaseModel):
    """Resultado de la validación del DAX/JSON generado."""
    is_valid: bool
    errors: list[str] = Field(default_factory=list)
    suggestions: list[str] = Field(default_factory=list)
    corrected_dax: str = ""


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    ORCHESTRATOR STATE                          ║
# ╚══════════════════════════════════════════════════════════════════╝


class OrchestratorState(BaseModel):
    """
    Estado del grafo LangGraph que fluye entre nodos.

    WHY: LangGraph opera con un estado tipado que pasa de nodo a nodo.
    Cada nodo lee lo que necesita del estado y escribe su resultado.
    Esto desacopla los nodos entre sí: el Router no sabe qué hace
    el Generator, solo escribe su clasificación en el estado.
    """
    # Input
    user_message: str = ""
    report_id: str = ""
    tenant_id: str = ""
    semantic_context: str = ""
    visual_context: list[dict[str, Any]] = Field(default_factory=list)

    # Router output
    intent: str = ""
    confidence: float = 0.0

    # Generator output
    actions: list[VisualAction] = Field(default_factory=list)
    action: VisualAction | None = None

    # Validator output
    is_valid: bool = False
    validation_errors: list[str] = Field(default_factory=list)

    # Control flow
    retry_count: int = 0
    max_retries: int = 2
    error_message: str = ""


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    API REQUEST/RESPONSE                        ║
# ╚══════════════════════════════════════════════════════════════════╝


class ChatRequest(BaseModel):
    """Input para POST /api/v1/chat."""
    message: str = Field(
        ...,
        min_length=1,
        max_length=2000,
        examples=["Crea un gráfico de barras comparando ventas por región"],
    )
    report_id: str = Field(..., examples=["uuid-del-reporte"])
    tenant_id: str = Field(..., examples=["uuid-del-tenant"])
    conversation_id: str | None = Field(
        default=None,
        examples=["uuid-conversation"],
        description="ID de la conversación existente. Si es null, se crea una nueva.",
    )
    visual_context: list[dict[str, Any]] = Field(
        default_factory=list,
        description=(
            "Contexto del lienzo actual enviado por frontend "
            "(id técnico, tipo y título de visuales disponibles)."
        ),
    )


class ChatResponse(BaseModel):
    """Output de POST /api/v1/chat."""
    status: str = "success"
    action: VisualAction
    actions: list[VisualAction] = Field(default_factory=list)
    intent: str
    confidence: float
    retries_used: int = 0
    conversation_id: str | None = None


class ExplainRequest(BaseModel):
    """Input para /api/v1/explain (pipeline híbrido Pandas + Gemini)."""
    visual_title: str | None = None
    raw_data: list[dict[str, Any]] = Field(default_factory=list)
    user_query: str | None = None
    # Retrocompatibilidad con el frontend actual
    visual_name: str | None = None
    visual_type: str | None = None
    data: list[dict[str, Any]] | None = None

    @model_validator(mode="before")
    @classmethod
    def normalize_legacy_fields(cls, data: Any) -> Any:
        """Acepta contrato nuevo y legacy sin romper el endpoint existente."""
        if not isinstance(data, dict):
            return data
        payload = dict(data)
        if not payload.get("visual_title") and payload.get("visual_name"):
            payload["visual_title"] = payload["visual_name"]
        if payload.get("raw_data") is None and payload.get("data") is not None:
            payload["raw_data"] = payload["data"]
        return payload

    @model_validator(mode="after")
    def ensure_raw_data_present(self) -> "ExplainRequest":
        """Garantiza que siempre exista raw_data canónico."""
        if not self.raw_data and self.data:
            self.raw_data = self.data
        return self
