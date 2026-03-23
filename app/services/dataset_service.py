from __future__ import annotations

import io
import json
import re
from typing import Any

import pandas as pd


def _clean_column_name(name: Any) -> str:
    text = str(name or "").strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\s\-\/%()]", "", text)
    return text.strip()


def _normalize_dtype(series: pd.Series) -> str:
    if pd.api.types.is_datetime64_any_dtype(series):
        return "Fecha"
    if pd.api.types.is_numeric_dtype(series):
        return "Numérico"
    return "Texto"


def _add_time_derivative_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Añade columnas temporales derivadas cuando detecta una columna fecha real.

    - Periodo_Mes: texto MM-YYYY para evitar jerarquía Auto-Date en ejes.
    - Mes_Index: entero YYYYMM para orden temporal y lógica de periodo anterior.
    - Año: entero YYYY.
    - Trimestre: texto T1..T4.
    - NombreMes: texto con el nombre del mes.
    - Mes_Num: entero 1..12.
    """
    enriched = df.copy()

    for column in list(enriched.columns):
        parsed = pd.to_datetime(enriched[column], errors="coerce", dayfirst=True)
        if parsed.notna().sum() == 0:
            continue

        if "Periodo_Mes" not in enriched.columns:
            enriched["Periodo_Mes"] = parsed.dt.strftime("%m-%Y")
        if "Mes_Index" not in enriched.columns:
            enriched["Mes_Index"] = (parsed.dt.year * 100 + parsed.dt.month).astype("Int64")
        if "Año" not in enriched.columns:
            enriched["Año"] = parsed.dt.year.astype("Int64")
        if "Trimestre" not in enriched.columns:
            quarter = parsed.dt.quarter
            enriched["Trimestre"] = quarter.map(
                lambda q: f"T{int(q)}" if pd.notna(q) else pd.NA
            ).astype("string")
        if "NombreMes" not in enriched.columns:
            month_names = {
                1: "Enero",
                2: "Febrero",
                3: "Marzo",
                4: "Abril",
                5: "Mayo",
                6: "Junio",
                7: "Julio",
                8: "Agosto",
                9: "Septiembre",
                10: "Octubre",
                11: "Noviembre",
                12: "Diciembre",
            }
            month_num = parsed.dt.month
            enriched["NombreMes"] = month_num.map(
                lambda m: month_names.get(int(m)) if pd.notna(m) else pd.NA
            ).astype("string")
        if "Mes_Num" not in enriched.columns:
            enriched["Mes_Num"] = parsed.dt.month.astype("Int64")
        break

    return enriched


def _summarize_dataframe(df: pd.DataFrame, table_name: str) -> dict[str, Any]:
    cleaned_df = df.copy()
    cleaned_df.columns = [_clean_column_name(col) for col in cleaned_df.columns]
    cleaned_df = _add_time_derivative_columns(cleaned_df)

    columns = [
        {
            "name": str(column),
            "dtype": _normalize_dtype(cleaned_df[column]),
        }
        for column in cleaned_df.columns
    ]

    # Usamos to_json de Pandas que maneja correctamente Timestamps y NaNs,
    # y luego lo volvemos a cargar como dict nativo.
    sample_json_str = cleaned_df.head(5).to_json(orient="records", date_format="iso")
    sample = json.loads(sample_json_str)

    # Pasar TODOS los periodos únicos para el bypass temporal
    periodo_unique: list[str] = []
    if "Periodo_Mes" in cleaned_df.columns:
        periodo_unique = sorted(cleaned_df["Periodo_Mes"].dropna().unique().tolist())

    return {
        "table_name": table_name,
        "row_count": int(len(cleaned_df.index)),
        "columns": columns,
        "sample": sample,
        "periodo_unique": periodo_unique,
    }


def _load_csv(file_content: bytes) -> dict[str, pd.DataFrame]:
    dataframe = pd.read_csv(io.BytesIO(file_content))
    return {"dataset": dataframe}


def _load_excel(file_content: bytes) -> dict[str, pd.DataFrame]:
    sheets = pd.read_excel(io.BytesIO(file_content), sheet_name=None)
    normalized: dict[str, pd.DataFrame] = {
        str(sheet_name).strip() or "Sheet": df
        for sheet_name, df in sheets.items()
    }

    signature_map: dict[tuple[str, ...], list[tuple[str, pd.DataFrame]]] = {}
    for sheet_name, df in normalized.items():
        cleaned_columns = tuple(_clean_column_name(col) for col in df.columns)
        signature_map.setdefault(cleaned_columns, []).append((sheet_name, df))

    output: dict[str, pd.DataFrame] = {}
    merged_index = 1
    for _, sheet_group in signature_map.items():
        if len(sheet_group) == 1:
            sheet_name, df = sheet_group[0]
            output[sheet_name] = df
            continue

        merged_name = f"merged_sheet_{merged_index}"
        merged_index += 1
        output[merged_name] = pd.concat([df for _, df in sheet_group], ignore_index=True)

    return output


async def process_uploaded_file(file_content: bytes, filename: str) -> dict[str, Any]:
    normalized_filename = str(filename or "").strip().lower()
    if normalized_filename.endswith(".csv"):
        datasets = _load_csv(file_content)
    elif normalized_filename.endswith(".xlsx") or normalized_filename.endswith(".xls"):
        datasets = _load_excel(file_content)
    else:
        raise ValueError("Formato no soportado. Solo se aceptan archivos CSV o Excel.")

    tables = [_summarize_dataframe(df, table_name) for table_name, df in datasets.items()]

    return {
        "filename": filename,
        "tables": tables,
    }
