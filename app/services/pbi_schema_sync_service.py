"""
Power BI Schema Sync Service — Sincroniza esquema real vía REST API.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from app.auth.power_bi_auth import PowerBIAuthManager
from app.db.supabase_client import get_supabase_client
from app.models.schemas import ColumnSchema
from app.services.schema_enrichment_service import enrich_columns_from_schema
from app.services.semantic_service import sync_schema

logger = logging.getLogger(__name__)

PBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"


def _normalize_pbi_type(raw: str | None) -> str:
    if not raw:
        return "Texto"
    value = str(raw).lower()
    if value in {"int64", "int", "integer", "decimal", "double", "number", "numeric"}:
        return "Numérico"
    if value in {"datetime", "date", "datetime64"}:
        return "Fecha"
    if value in {"boolean", "bool"}:
        return "Booleano"
    return "Texto"


def _extract_columns_from_scan(scan_result: dict[str, Any], dataset_id: str) -> list[ColumnSchema]:
    workspaces = scan_result.get("workspaces", [])
    for workspace in workspaces:
        datasets = workspace.get("datasets", []) if isinstance(workspace, dict) else []
        for dataset in datasets:
            if str(dataset.get("id", "")).strip() != str(dataset_id):
                continue

            tables = dataset.get("tables", [])
            if not tables and isinstance(dataset.get("datasetSchema"), dict):
                tables = dataset["datasetSchema"].get("tables", [])

            columns_out: list[ColumnSchema] = []
            for table in tables or []:
                table_name = str(table.get("name", "")).strip()
                if not table_name:
                    continue

                for col in table.get("columns", []) or []:
                    col_name = str(col.get("name", "")).strip()
                    if not col_name:
                        continue
                    columns_out.append(
                        ColumnSchema(
                            table_name=table_name,
                            column_name=col_name,
                            data_type=_normalize_pbi_type(col.get("dataType")),
                            description="",
                            is_measure=False,
                            dax_expression="",
                            sample_values=[],
                            metadata={"source": "powerbi_scan"},
                        )
                    )

                for measure in table.get("measures", []) or []:
                    measure_name = str(measure.get("name", "")).strip()
                    if not measure_name:
                        continue
                    columns_out.append(
                        ColumnSchema(
                            table_name=table_name,
                            column_name=measure_name,
                            data_type=_normalize_pbi_type(measure.get("dataType")) or "Measure",
                            description="",
                            is_measure=True,
                            dax_expression=str(measure.get("expression", "") or ""),
                            sample_values=[],
                            metadata={"source": "powerbi_scan", "kind": "measure"},
                        )
                    )

            return columns_out

    return []


def _extract_columns_from_tables(tables: list[dict[str, Any]]) -> list[ColumnSchema]:
    columns_out: list[ColumnSchema] = []
    for table in tables or []:
        table_name = str(table.get("name", "")).strip()
        if not table_name:
            continue

        for col in table.get("columns", []) or []:
            col_name = str(col.get("name", "")).strip()
            if not col_name:
                continue
            columns_out.append(
                ColumnSchema(
                    table_name=table_name,
                    column_name=col_name,
                    data_type=_normalize_pbi_type(col.get("dataType")),
                    description="",
                    is_measure=False,
                    dax_expression="",
                    sample_values=[],
                    metadata={"source": "powerbi_tables"},
                )
            )

    return columns_out


def _pick_dataset_id(report_payload: dict[str, Any], preferred_dataset_id: str | None) -> str | None:
    dataset_id = str(report_payload.get("datasetId") or "").strip()
    if dataset_id:
        return dataset_id

    raw = report_payload.get("datasetIds")
    if isinstance(raw, list):
        dataset_ids = [str(x).strip() for x in raw if str(x).strip()]
        if not dataset_ids:
            return None
        if preferred_dataset_id and preferred_dataset_id in dataset_ids:
            return preferred_dataset_id
        return dataset_ids[0]

    return None


class AdminSchemaBlockedError(RuntimeError):
    """El tenant bloquea /admin APIs (Scanner API)."""


class SchemaReadBlockedError(RuntimeError):
    """
    El tenant/cuenta bloquea la lectura de esquema vía executeQueries/DMVs.
    (Típico en licencias Pro + modelos Import con gobernanza/restricciones).
    """


def _normalize_dmv_type(raw: Any) -> str:
    """
    Normaliza DATA_TYPE de DMVs (a veces viene como string, a veces como int OLE DB).
    """
    if raw is None:
        return "Texto"

    # OLE DB type codes (subset útil para BI)
    try:
        code = int(raw)
        if code in {2, 3, 4, 5, 6, 7, 16, 17, 18, 19, 20, 21, 131}:
            return "Numérico"
        if code in {11}:
            return "Booleano"
        if code in {7, 133, 134, 135}:
            return "Fecha"
    except (TypeError, ValueError):
        pass

    return _normalize_pbi_type(str(raw))


def _extract_columns_from_execute_queries(payload: dict[str, Any]) -> list[ColumnSchema]:
    """
    Extrae columnas desde la respuesta de executeQueries.
    Espera results[0].tables[0].rows como lista de dicts.
    """
    results = payload.get("results") or []
    if not isinstance(results, list) or not results:
        return []

    tables = (results[0] or {}).get("tables") or []
    if not isinstance(tables, list) or not tables:
        return []

    rows = (tables[0] or {}).get("rows") or []
    if not isinstance(rows, list) or not rows:
        return []

    # Spy (dev-friendly): muestra cómo viene el DMV sin spamear columnas.
    try:
        sample_tables: list[str] = []
        for row in rows[:2000]:
            if not isinstance(row, dict):
                continue
            t = str(row.get("TABLE_NAME") or row.get("Table") or row.get("TABLE") or "").strip()
            if t:
                sample_tables.append(t)
        uniq = sorted(set(sample_tables))
        logger.info(
            "PBI DMV rows=%d distinct_tables=%d sample_tables=%s",
            len(rows),
            len(uniq),
            ", ".join(uniq[:25]),
        )
    except Exception:
        logger.debug("PBI DMV spy falló", exc_info=True)

    columns_out: list[ColumnSchema] = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        table_name = str(row.get("TABLE_NAME") or row.get("Table") or row.get("TABLE") or "").strip()
        col_name = str(row.get("COLUMN_NAME") or row.get("Column") or row.get("COLUMN") or "").strip()
        data_type = row.get("DATA_TYPE") or row.get("DataType") or row.get("DATA_TYPE_NAME")

        if not table_name or not col_name:
            continue
        if (
            table_name.startswith("DateTableTemplate")
            or table_name.startswith("LocalDateTable")
            or table_name.startswith("DateTemplate")
        ):
            continue

        columns_out.append(
            ColumnSchema(
                table_name=table_name,
                column_name=col_name,
                data_type=_normalize_dmv_type(data_type),
                description="",
                is_measure=False,
                dax_expression="",
                sample_values=[],
                metadata={"source": "powerbi_executeQueries"},
            )
        )

    return columns_out


def _extract_measures_from_execute_queries(payload: dict[str, Any]) -> list[ColumnSchema]:
    """
    Extrae medidas desde la respuesta de executeQueries.
    Espera results[0].tables[0].rows con TABLE_NAME, MEASURE_NAME y opcionalmente EXPRESSION/DAX_EXPRESSION.
    """
    results = payload.get("results") or []
    if not isinstance(results, list) or not results:
        return []

    tables = (results[0] or {}).get("tables") or []
    if not isinstance(tables, list) or not tables:
        return []

    rows = (tables[0] or {}).get("rows") or []
    if not isinstance(rows, list) or not rows:
        return []

    out: list[ColumnSchema] = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        table_name = str(row.get("TABLE_NAME") or row.get("Table") or row.get("TABLE") or "").strip()
        measure_name = str(
            row.get("MEASURE_NAME")
            or row.get("MEASURE")
            or row.get("COLUMN_NAME")
            or row.get("Name")
            or ""
        ).strip()
        data_type = row.get("DATA_TYPE") or row.get("DataType") or row.get("DATA_TYPE_NAME")
        expression = str(row.get("EXPRESSION") or row.get("DAX_EXPRESSION") or "").strip()

        if not table_name or not measure_name:
            continue
        if (
            table_name.startswith("DateTableTemplate")
            or table_name.startswith("LocalDateTable")
            or table_name.startswith("DateTemplate")
        ):
            continue

        out.append(
            ColumnSchema(
                table_name=table_name,
                column_name=measure_name,
                data_type=_normalize_dmv_type(data_type) or "Measure",
                description="",
                is_measure=True,
                dax_expression=expression,
                sample_values=[],
                metadata={"source": "powerbi_executeQueries", "kind": "measure"},
            )
        )

    return out


def _extract_rows_from_execute_queries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    results = payload.get("results") or []
    if not isinstance(results, list) or not results:
        return []
    tables = (results[0] or {}).get("tables") or []
    if not isinstance(tables, list) or not tables:
        return []
    rows = (tables[0] or {}).get("rows") or []
    if not isinstance(rows, list):
        return []
    return [r for r in rows if isinstance(r, dict)]


async def sync_schema_from_powerbi(
    report_id: str,
    tenant_id: str,
    powerbi_access_token: str | None = None,
) -> tuple[int, list[ColumnSchema]]:
    client = get_supabase_client()
    report = (
        client.table("reports")
        .select("pbi_workspace_id, pbi_dataset_id, pbi_report_id")
        .eq("id", report_id)
        .eq("tenant_id", tenant_id)
        .single()
        .execute()
    )

    if not report.data:
        raise ValueError("Reporte no encontrado para el tenant.")

    workspace_id = str(report.data["pbi_workspace_id"]).strip()
    stored_dataset_id = str(report.data.get("pbi_dataset_id") or "").strip()
    pbi_report_id = str(report.data["pbi_report_id"]).strip()

    access_token = powerbi_access_token or await PowerBIAuthManager().acquire_token()
    using_delegated_token = powerbi_access_token is not None

    async with httpx.AsyncClient(
        base_url=PBI_API_BASE,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        timeout=30.0,
    ) as http:
        # Descubrimiento robusto del datasetId real desde el reporte:
        # evita depender de valores manuales en Supabase (causa común de 404).
        report_resp = await http.get(f"/groups/{workspace_id}/reports/{pbi_report_id}")
        if report_resp.status_code >= 400:
            # Si falla el descubrimiento, seguimos con lo almacenado (si existe)
            # para no romper el flujo en tenants donde el reporte no sea accesible.
            discovered_dataset_id = stored_dataset_id or ""
        else:
            report_payload = report_resp.json() or {}
            discovered_dataset_id = _pick_dataset_id(report_payload, stored_dataset_id) or ""

        dataset_id = discovered_dataset_id or stored_dataset_id
        if not dataset_id:
            raise RuntimeError("No se pudo descubrir el datasetId del reporte en Power BI.")

        if dataset_id and dataset_id != stored_dataset_id:
            try:
                client.table("reports").update({"pbi_dataset_id": dataset_id}).eq("id", report_id).eq(
                    "tenant_id", tenant_id
                ).execute()
            except Exception:
                logger.warning("No se pudo persistir pbi_dataset_id descubierto en Supabase.", exc_info=True)

        # Intento 1 (Enterprise): Scanner API (requiere habilitación de /admin APIs en el tenant).
        # Si el tenant lo bloquea, retornamos "operational" para que el frontend haga fallback SDK.
        if not using_delegated_token:
            scan_resp = await http.post(
                "/admin/workspaces/getInfo",
                params={"lineage": "true", "datasetSchema": "true"},
                json={"workspaces": [workspace_id]},
            )

            if scan_resp.status_code in {401, 403}:
                raise AdminSchemaBlockedError("Tenant bloquea Scanner API (/admin).")

            if scan_resp.status_code < 400:
                scan_payload = scan_resp.json() or {}
                scan_id = scan_payload.get("id") or scan_payload.get("scanId") or scan_payload.get("scan_id")
                if not scan_id:
                    raise RuntimeError("Power BI scan no retornó un id válido.")

                status = "running"
                for _ in range(24):
                    await asyncio.sleep(1.5)
                    status_resp = await http.get(f"/admin/workspaces/scanStatus/{scan_id}")
                    if status_resp.status_code >= 400:
                        raise RuntimeError(f"Power BI scanStatus falló: {status_resp.text}")
                    status_payload = status_resp.json() or {}
                    status = str(status_payload.get("status", "")).lower()
                    if status in {"succeeded", "failed"}:
                        break

                if status != "succeeded":
                    raise RuntimeError("Power BI scan no pudo completarse correctamente.")

                result_resp = await http.get(f"/admin/workspaces/scanResult/{scan_id}")
                if result_resp.status_code >= 400:
                    raise RuntimeError(f"Power BI scanResult falló: {result_resp.text}")

                scan_result = result_resp.json() or {}
                columns = _extract_columns_from_scan(scan_result, dataset_id)

                if columns:
                    columns = await enrich_columns_from_schema(columns)
                    synced_count = await sync_schema(
                        report_id=report_id,
                        tenant_id=tenant_id,
                        columns=columns,
                    )
                    logger.info(
                        "🧭 Schema PBI sincronizado (Scanner API): report=%s tenant=%s columnas=%d",
                        report_id,
                        tenant_id,
                        synced_count,
                    )
                    return synced_count, columns

        # Esquema para datasets Import/DirectQuery: executeQueries con DMV.
        # Nota: no requiere Admin APIs; solo permisos Build/Read sobre el dataset.
        # executeQueries acepta consultas DMV estilo SQL cuando el query empieza con SELECT.
        # No mezclar con DAX (EVALUATE/DEFINE), o el parser lo interpreta como DAX y falla.
        dmv_query = "SELECT [TABLE_NAME], [COLUMN_NAME], [DATA_TYPE] FROM $SYSTEM.DBSCHEMA_COLUMNS"
        exec_payload = {
            "queries": [{"query": dmv_query}],
            "serializerSettings": {"includeNulls": True},
        }

        exec_resp = await http.post(
            f"/groups/{workspace_id}/datasets/{dataset_id}/executeQueries",
            json=exec_payload,
        )
        if exec_resp.status_code == 404:
            exec_resp = await http.post(f"/datasets/{dataset_id}/executeQueries", json=exec_payload)

        if exec_resp.status_code < 400:
            columns = _extract_columns_from_execute_queries(exec_resp.json() or {})
        else:
            logger.error(
                "Power BI executeQueries error (%d) [DMV]. Body: %s",
                exec_resp.status_code,
                exec_resp.text,
            )
            # Fallback defensivo: algunas capacidades aceptan solo DAX (INFO.COLUMNS()).
            alt_payload = {
                "queries": [{"query": "EVALUATE INFO.COLUMNS()"}],
                "serializerSettings": {"includeNulls": True},
            }
            alt_resp = await http.post(
                f"/groups/{workspace_id}/datasets/{dataset_id}/executeQueries",
                json=alt_payload,
            )
            if alt_resp.status_code == 404:
                alt_resp = await http.post(f"/datasets/{dataset_id}/executeQueries", json=alt_payload)

            if alt_resp.status_code < 400:
                columns = _extract_columns_from_execute_queries(alt_resp.json() or {})
            else:
                if using_delegated_token:
                    logger.error(
                        "Power BI executeQueries error (%d) [INFO.COLUMNS]. Body: %s",
                        alt_resp.status_code,
                        alt_resp.text,
                    )
                    raise SchemaReadBlockedError(
                        "Bloqueado: no se pudo leer el esquema del dataset vía executeQueries."
                    )
                columns = []

        # Si DBSCHEMA_COLUMNS devuelve solo tablas internas o nada, usar DMVs TMSCHEMA sin JOINs.
        # executeQueries no tolera aliases/joins; hacemos 2-3 SELECT simples y unimos en Python.
        if not columns:
            table_map: dict[str, str] = {}

            tables_query = "SELECT [ID], [Name] FROM $SYSTEM.TMSCHEMA_TABLES"
            tables_payload = {"queries": [{"query": tables_query}], "serializerSettings": {"includeNulls": True}}
            t_resp = await http.post(
                f"/groups/{workspace_id}/datasets/{dataset_id}/executeQueries",
                json=tables_payload,
            )
            if t_resp.status_code == 404:
                t_resp = await http.post(f"/datasets/{dataset_id}/executeQueries", json=tables_payload)

            if t_resp.status_code < 400:
                for row in _extract_rows_from_execute_queries(t_resp.json() or {}):
                    rid = str(row.get("ID") or row.get("Id") or "").strip()
                    name = str(row.get("Name") or row.get("NAME") or "").strip()
                    if rid and name:
                        table_map[rid] = name
            else:
                logger.error(
                    "Power BI executeQueries error (%d) [TMSCHEMA_TABLES]. Body: %s",
                    t_resp.status_code,
                    t_resp.text,
                )

            cols_query = "SELECT [TableID], [Name], [DataType] FROM $SYSTEM.TMSCHEMA_COLUMNS"
            cols_payload = {"queries": [{"query": cols_query}], "serializerSettings": {"includeNulls": True}}
            c_resp = await http.post(
                f"/groups/{workspace_id}/datasets/{dataset_id}/executeQueries",
                json=cols_payload,
            )
            if c_resp.status_code == 404:
                c_resp = await http.post(f"/datasets/{dataset_id}/executeQueries", json=cols_payload)

            if c_resp.status_code < 400 and table_map:
                tms_columns: list[ColumnSchema] = []
                for row in _extract_rows_from_execute_queries(c_resp.json() or {}):
                    tid = str(row.get("TableID") or row.get("TABLEID") or "").strip()
                    col = str(row.get("Name") or row.get("NAME") or "").strip()
                    dtype = row.get("DataType") or row.get("DATATYPE")
                    tname = table_map.get(tid, "").strip()
                    if not tname or not col:
                        continue
                    if (
                        tname.startswith("DateTableTemplate")
                        or tname.startswith("LocalDateTable")
                        or tname.startswith("DateTemplate")
                    ):
                        continue
                    tms_columns.append(
                        ColumnSchema(
                            table_name=tname,
                            column_name=col,
                            data_type=_normalize_dmv_type(dtype),
                            description="",
                            is_measure=False,
                            dax_expression="",
                            sample_values=[],
                            metadata={"source": "powerbi_executeQueries", "kind": "column"},
                        )
                    )
                columns = tms_columns
            elif c_resp.status_code >= 400:
                logger.error(
                    "Power BI executeQueries error (%d) [TMSCHEMA_COLUMNS]. Body: %s",
                    c_resp.status_code,
                    c_resp.text,
                )

            measures: list[ColumnSchema] = []
            if columns and table_map:
                measures_query = "SELECT [TableID], [Name], [DataType], [Expression] FROM $SYSTEM.TMSCHEMA_MEASURES"
                measures_payload = {"queries": [{"query": measures_query}], "serializerSettings": {"includeNulls": True}}
                m_resp = await http.post(
                    f"/groups/{workspace_id}/datasets/{dataset_id}/executeQueries",
                    json=measures_payload,
                )
                if m_resp.status_code == 404:
                    m_resp = await http.post(f"/datasets/{dataset_id}/executeQueries", json=measures_payload)
                if m_resp.status_code < 400:
                    for row in _extract_rows_from_execute_queries(m_resp.json() or {}):
                        tid = str(row.get("TableID") or row.get("TABLEID") or "").strip()
                        name = str(row.get("Name") or row.get("NAME") or "").strip()
                        dtype = row.get("DataType") or row.get("DATATYPE")
                        expr = str(row.get("Expression") or row.get("EXPRESSION") or "").strip()
                        tname = table_map.get(tid, "").strip()
                        if not tname or not name:
                            continue
                        if (
                            tname.startswith("DateTableTemplate")
                            or tname.startswith("LocalDateTable")
                            or tname.startswith("DateTemplate")
                        ):
                            continue
                        measures.append(
                            ColumnSchema(
                                table_name=tname,
                                column_name=name,
                                data_type=_normalize_dmv_type(dtype) or "Measure",
                                description="",
                                is_measure=True,
                                dax_expression=expr,
                                sample_values=[],
                                metadata={"source": "powerbi_executeQueries", "kind": "measure"},
                            )
                        )
                else:
                    logger.error(
                        "Power BI executeQueries error (%d) [TMSCHEMA_MEASURES]. Body: %s",
                        m_resp.status_code,
                        m_resp.text,
                    )

            if measures:
                columns = columns + measures

    if not columns:
        if using_delegated_token:
            raise SchemaReadBlockedError(
                "Bloqueado: no se encontraron columnas accesibles en el modelo de Power BI."
            )
        raise RuntimeError("No se encontraron columnas en el modelo de Power BI.")

    columns = await enrich_columns_from_schema(columns)

    synced_count = await sync_schema(
        report_id=report_id,
        tenant_id=tenant_id,
        columns=columns,
    )

    logger.info(
        "🧭 Schema PBI sincronizado: report=%s tenant=%s columnas=%d",
        report_id,
        tenant_id,
        synced_count,
    )
    return synced_count, columns
