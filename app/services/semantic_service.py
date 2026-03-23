"""
Semantic Service — Retrieval Tool para el Diccionario Semántico.

WHY: Este servicio es el puente entre Supabase y el prompt de Gemini.
Cuando el orquestador necesita saber qué tablas/columnas existen en
un reporte, llama a este servicio en lugar de hacer queries SQL directas.

Esto cumple dos principios de ANTIGRAVITY_RULES:
1. Single Responsibility: la lógica de acceso a datos semánticos
   está encapsulada aquí, no dispersa en los endpoints.
2. Open/Closed: si mañana cambiamos el formato del diccionario para
   Gemini, solo modificamos format_for_prompt() sin tocar los endpoints.
"""

from __future__ import annotations

import logging
from typing import Any

from app.db.supabase_client import get_supabase_client
from app.models.schemas import ColumnSchema, SemanticDictionaryResponse

logger = logging.getLogger(__name__)

_NUMERIC_TOKENS = {
    "decimal", "double", "float", "number", "numeric", "int", "int64", "integer",
    "whole number", "entero", "numero", "número",
}
_DATE_TOKENS = {"date", "datetime", "datetime64", "fecha", "hora", "timestamp"}
_TEXT_TOKENS = {"string", "text", "texto", "varchar", "nvarchar", "char"}


def _normalize_type(value: str) -> str:
    return str(value or "").strip().lower()


def _type_rank(value: str) -> int:
    """
    Ranking de calidad del tipo de dato.
    3 = numérico, 2 = fecha, 1 = texto, 0 = desconocido.
    """
    t = _normalize_type(value)
    if not t:
        return 0
    if any(tok in t for tok in _NUMERIC_TOKENS):
        return 3
    if any(tok in t for tok in _DATE_TOKENS):
        return 2
    if any(tok in t for tok in _TEXT_TOKENS):
        return 1
    return 0


def _is_weak_type(value: str) -> bool:
    t = _normalize_type(value)
    return (not t) or t in {"unknown", "desconocido"} or any(tok in t for tok in _TEXT_TOKENS)


async def save_uploaded_schema(
    tenant_id: str,
    report_id: str,
    tables_info: list[dict[str, Any]],
) -> int:
    """
    Persiste el esquema detectado desde un archivo subido.

    Estrategia:
    - Borra el diccionario anterior del reporte para evitar duplicados.
    - Inserta columnas detectadas por tabla.
    """
    client = get_supabase_client()

    records: list[dict[str, Any]] = []

    for table in tables_info:
        if not isinstance(table, dict):
            continue
        table_name = str(table.get("table_name", "") or "").strip()
        columns = table.get("columns", [])
        sample_rows = table.get("sample", [])
        if not table_name or not isinstance(columns, list):
            continue

        for column in columns:
            if not isinstance(column, dict):
                continue
            column_name = str(column.get("name", "") or "").strip()
            data_type = str(column.get("dtype", "") or "").strip()
            if not column_name or not data_type:
                continue

            sample_values: list[Any] = []
            if column_name == "Periodo_Mes":
                # Usar los periodos únicos del dataframe completo (no head(5))
                periodo_unique = table.get("periodo_unique", [])
                if isinstance(periodo_unique, list) and periodo_unique:
                    sample_values = periodo_unique
                elif isinstance(sample_rows, list):
                    seen: set[str] = set()
                    for row in sample_rows:
                        if isinstance(row, dict) and column_name in row:
                            val = row.get(column_name)
                            if val is not None and str(val) not in seen:
                                seen.add(str(val))
                                sample_values.append(val)
            elif isinstance(sample_rows, list):
                for row in sample_rows[:5]:
                    if isinstance(row, dict) and column_name in row:
                        sample_values.append(row.get(column_name))

            records.append(
                {
                    "tenant_id": tenant_id,
                    "report_id": report_id,
                    "table_name": table_name,
                    "column_name": column_name,
                    "data_type": data_type,
                    "description": "",
                    "is_measure": False,
                    "dax_expression": "",
                    "sample_values": sample_values,
                    "metadata": {"source": "upload_dataset"},
                }
            )

    (
        client.table("semantic_dictionaries")
        .delete()
        .eq("tenant_id", tenant_id)
        .eq("report_id", report_id)
        .execute()
    )

    if not records:
        logger.warning(
            "⚠️ No se detectaron columnas para persistir: report=%s tenant=%s",
            report_id,
            tenant_id,
        )
        return 0

    result = client.table("semantic_dictionaries").insert(records).execute()
    synced_count = len(result.data) if result.data else len(records)

    client.table("reports").update(
        {"schema_version": _get_next_schema_version(report_id)}
    ).eq("id", report_id).execute()

    logger.info(
        "🧩 Esquema de archivo persistido: report=%s, tenant=%s, columnas=%d",
        report_id,
        tenant_id,
        synced_count,
    )
    return synced_count


async def sync_schema(
    report_id: str,
    tenant_id: str,
    columns: list[ColumnSchema],
) -> int:
    """
    Sincroniza el esquema de Power BI en Supabase.

    WHY: Usa upsert (INSERT ... ON CONFLICT UPDATE) para que se pueda
    re-sincronizar sin duplicar registros. Si una columna ya existe,
    se actualiza su tipo/descripción. Si es nueva, se inserta.

    Args:
        report_id: UUID del reporte registrado.
        tenant_id: UUID del tenant propietario.
        columns: Lista de columnas del modelo de Power BI.

    Returns:
        Número de columnas sincronizadas.
    """
    client = get_supabase_client()

    if not columns:
        logger.info(
            "⚠️ sync_schema: lista vacía (no-op). report=%s tenant=%s",
            report_id,
            tenant_id,
        )
        return 0

    # Cargar esquema existente para evitar downgrades.
    # WHY: El fallback operacional (SDK) no expone tipos confiables.
    existing_map: dict[tuple[str, str], dict[str, Any]] = {}
    try:
        existing_res = (
            client.table("semantic_dictionaries")
            .select("table_name,column_name,data_type,metadata")
            .eq("report_id", report_id)
            .eq("tenant_id", tenant_id)
            .execute()
        )
        for row in (existing_res.data or []):
            t = str(row.get("table_name", "") or "").strip().lower()
            c = str(row.get("column_name", "") or "").strip().lower()
            if t and c:
                existing_map[(t, c)] = row
    except Exception as exc:
        logger.warning("⚠️ sync_schema: no se pudo cargar esquema existente (%s).", exc)
    
    # Preparar payload JSON para el RPC o fallback
    columns_json = [
        {
            "table_name": col.table_name,
            "column_name": col.column_name,
            "data_type": col.data_type,
            "description": col.description,
            "is_measure": col.is_measure,
            "dax_expression": col.dax_expression,
            "sample_values": col.sample_values,
            "metadata": col.metadata,
        }
        for col in columns
    ]

    # Soft-merge de tipos por confianza.
    for item in columns_json:
        md_in = dict(item.get("metadata") or {})
        src = str(md_in.get("source") or "").strip().lower()

        key = (str(item.get("table_name") or "").strip().lower(), str(item.get("column_name") or "").strip().lower())
        prev = existing_map.get(key) or {}
        prev_type = str(prev.get("data_type") or "")
        next_type = str(item.get("data_type") or "")

        prev_rank = _type_rank(prev_type)
        next_rank = _type_rank(next_type)

        # Operational sync: no permitir que "Texto/Unknown" pise un tipo fuerte.
        if src == "sdk_operational" and _is_weak_type(next_type) and prev_rank > next_rank:
            item["data_type"] = prev_type
            md_prev = dict(prev.get("metadata") or {})
            md_prev.update(md_in)
            md_prev["type_source"] = md_prev.get("type_source") or md_prev.get("source") or "unknown"
            item["metadata"] = md_prev
            continue

        # Si el nuevo tipo es mejor o igual y no es desconocido, registrar fuente del tipo.
        if next_rank >= prev_rank and next_rank > 0:
            md_in["type_source"] = md_in.get("type_source") or md_in.get("source") or "unknown"
        item["metadata"] = md_in

    # 1. Intentar método ATÓMICO (Phase 5 Rule)
    try:
        # WHY: LLamamos al RPC creado en 20260218_atomic_sync_schema.sql.
        # Esto garantiza que el upsert y el update de versión sean una sola transacción.
        result = client.rpc(
            "sync_schema_atomic",
            {
                "p_report_id": report_id,
                "p_tenant_id": tenant_id,
                "p_columns": columns_json,
            },
        ).execute()
        
        synced_count = result.data
        logger.info(
            "⚛️ Schema sincronizado ATÓMICAMENTE: report=%s, tenant=%s, columnas=%d",
            report_id,
            tenant_id,
            synced_count,
        )
        return synced_count

    except Exception as exc:
        # 2. Fallback a método SECUENCIAL (Backward Compatibility)
        # Si el RPC no existe (migración no corrida) o falla, usamos el método antiguo.
        logger.warning(
            "⚠️ Falló sync atómico (%s). Usando fallback secuencial.", exc
        )

        # Preparar registros para upsert (agregando IDs faltantes)
        records = []
        for col_data in columns_json:
            record = col_data.copy()
            record["report_id"] = report_id
            record["tenant_id"] = tenant_id
            records.append(record)

        # Upsert diccionario
        upsert_res = (
            client.table("semantic_dictionaries")
            .upsert(records, on_conflict="report_id,table_name,column_name")
            .execute()
        )
        synced_count = len(upsert_res.data) if upsert_res.data else 0

        # Update versión
        client.table("reports").update(
            {"schema_version": _get_next_schema_version(report_id)}
        ).eq("id", report_id).execute()

        logger.info(
            "📚 Schema sincronizado (Secuencial): report=%s, tenant=%s, columnas=%d",
            report_id,
            tenant_id,
            synced_count,
        )
        return synced_count


def _get_next_schema_version(report_id: str) -> int:
    """
    Obtiene la versión actual del schema e incrementa en 1.

    WHY: El schema_version se usa como parte de la key del cache
    semántico (Power Upgrade U2). Cuando cambia el schema, el cache
    se invalida automáticamente.
    """
    client = get_supabase_client()
    result = (
        client.table("reports")
        .select("schema_version")
        .eq("id", report_id)
        .single()
        .execute()
    )
    current = result.data.get("schema_version", 1) if result.data else 1
    return current + 1


async def get_semantic_dictionary(
    report_id: str,
    tenant_id: str,
) -> SemanticDictionaryResponse | None:
    """
    Recupera el diccionario semántico completo de un reporte.

    WHY: Este es el "Retrieval Tool" del Plan Maestro. Cuando el
    orquestador LangGraph recibe una solicitud del usuario, primero
    llama a esta función para obtener el contexto de qué datos existen.
    Luego pasa este diccionario al prompt de Gemini para garantizar
    zero-hallucinations.

    Args:
        report_id: UUID del reporte.
        tenant_id: UUID del tenant (seguridad multi-tenant).

    Returns:
        SemanticDictionaryResponse con tablas agrupadas, o None si
        el reporte no tiene schema sincronizado.
    """
    client = get_supabase_client()

    # Obtener metadata del reporte
    report_result = (
        client.table("reports")
        .select("id, name, schema_version")
        .eq("id", report_id)
        .eq("tenant_id", tenant_id)
        .single()
        .execute()
    )

    if not report_result.data:
        logger.warning(
            "⚠️ Reporte no encontrado: report=%s, tenant=%s",
            report_id,
            tenant_id,
        )
        return None

    report_data = report_result.data

    # Obtener todas las columnas del diccionario semántico
    # WHY: Filtramos por tenant_id además de report_id como
    # doble verificación de seguridad multi-tenant.
    columns_result = (
        client.table("semantic_dictionaries")
        .select("*")
        .eq("report_id", report_id)
        .eq("tenant_id", tenant_id)
        .order("table_name")
        .order("column_name")
        .execute()
    )

    if not columns_result.data:
        logger.warning(
            "⚠️ Schema vacío para reporte: %s", report_id
        )
        return None

    # Agrupar columnas por tabla
    # WHY: Este formato agrupado es más eficiente para el prompt de
    # Gemini. En lugar de una lista plana de 100 columnas, Gemini
    # recibe un diccionario organizado por tabla, lo cual mejora
    # la precisión en la generación de DAX.
    tables: dict[str, list[ColumnSchema]] = {}
    for row in columns_result.data:
        table_name = row["table_name"]
        col = ColumnSchema(
            table_name=row["table_name"],
            column_name=row["column_name"],
            data_type=row["data_type"],
            description=row.get("description", ""),
            is_measure=row.get("is_measure", False),
            dax_expression=row.get("dax_expression", ""),
            sample_values=row.get("sample_values", []),
            metadata=row.get("metadata", {}),
        )
        tables.setdefault(table_name, []).append(col)

    total_columns = sum(len(cols) for cols in tables.values())

    logger.info(
        "📖 Diccionario semántico cargado: report=%s, tablas=%d, columnas=%d",
        report_id,
        len(tables),
        total_columns,
    )

    return SemanticDictionaryResponse(
        report_id=report_id,
        report_name=report_data.get("name", ""),
        schema_version=report_data.get("schema_version", 1),
        tables=tables,
        total_columns=total_columns,
    )


def format_dictionary_for_prompt(
    dictionary: SemanticDictionaryResponse,
) -> str:
    """
    Formatea el diccionario semántico como texto para el System Prompt de Gemini.

    WHY: Gemini necesita el esquema en texto plano dentro del prompt,
    no como JSON anidado. Este formato tabular es más eficiente en
    tokens y más fácil de parsear para el LLM.

    Ejemplo de output:
        ## Tabla: Ventas
        | Columna | Tipo | Medida | Descripción |
        |---------|------|--------|-------------|
        | Monto   | Decimal | No  | Monto total de la venta |
        | Total Ventas | Decimal | Sí | SUM(Ventas[Monto]) |
    """
    lines: list[str] = [
        f"# Diccionario de Datos: {dictionary.report_name}",
        f"Schema Version: {dictionary.schema_version}",
        f"Total Columnas: {dictionary.total_columns}",
        "",
    ]

    for table_name, columns in dictionary.tables.items():
        lines.append(f"## Tabla: {table_name}")
        lines.append("| Columna | Tipo | Medida | DAX | Valores Ejemplo | Descripción |")
        lines.append("|---------|------|--------|-----|-----------------|-------------|")

        for col in columns:
            is_measure = "Sí" if col.is_measure else "No"
            dax = col.dax_expression if col.dax_expression else "—"
            dtype = col.data_type
            src = str((col.metadata or {}).get("source") or "").strip().lower()
            # Tipos desde operational sync no son confiables: evita inducir al LLM a decisiones erróneas.
            if src == "sdk_operational" and _is_weak_type(dtype):
                dtype = "Unknown"
            # FASE 5.2: Incluir hasta 3 valores únicos de ejemplo para que
            # Gemini pueda generar filtros con valores reales del dataset
            # (ej: saber los Periodo_Mes disponibles para Time Intelligence).
            sample_str = "—"
            if col.sample_values:
                unique_samples = list(dict.fromkeys(
                    str(v) for v in col.sample_values if v is not None
                ))[:3]
                if unique_samples:
                    sample_str = ", ".join(unique_samples)
            lines.append(
                f"| {col.column_name} | {dtype} | "
                f"{is_measure} | {dax} | {sample_str} | {col.description} |"
            )
        lines.append("")

    return "\n".join(lines)
