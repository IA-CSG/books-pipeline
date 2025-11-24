# src/integrate_pipeline.py

import hashlib
import json
import os
from datetime import datetime, UTC
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from utils_isbn import clean_isbn, is_isbn13, is_isbn10, to_isbn13, normalize_isbn13
from utils_quality import compute_null_percentages, compute_basic_counts, count_duplicates

# ------------------------------------------------------------
# Carga ficheros fuente (JSON y CSV) - SOLO LECTURA EN landing/
# ------------------------------------------------------------

def load_sources() -> tuple[pd.DataFrame, pd.DataFrame]:
    # Goodreads JSON → Forzamos tipos a STRING
    df_gr = pd.read_json(
        "landing/goodreads_books.json",
        orient="records",
        dtype={"isbn10": "string", "isbn13": "string", "asin": "string"}
    )

    # Google Books CSV
    df_gb = pd.read_csv(
        "landing/googlebooks_books.csv",
        delimiter=";",
        encoding="utf-8",
        dtype={"isbn10": "string", "isbn13": "string", "asin": "string"},
        low_memory=False,
    )

    return df_gr, df_gb


# ------------------------------------------------------------
# Funciones de normalización
# ------------------------------------------------------------

# Devuelve el idioma en minúsculas (BCP-47 simplificado) o None si viene vacío/NaN.
# Soporta valores float (NaN) sin romper.
def normalize_language(lang):
    if pd.isna(lang):
        return None
    lang_str = str(lang).strip()
    if not lang_str:
        return None
    return lang_str.lower()

#Devuelve la moneda en formato ISO-4217 (tres letras mayúsculas) o None.
def normalize_currency(cur) -> str | None:
    if pd.isna(cur) or cur is None:
        return None
    cur_str = str(cur).strip().upper()
    return cur_str if cur_str else None

# Convierte formatos típicos (YYYY, YYYY-MM, YYYY-MM-DD) a ISO-8601 YYYY-MM-DD.
# Si sólo hay año o año-mes, pandas rellena día/mes al 1 (comportamiento aceptable aquí).
def normalize_date(date_str) -> str | None:       
    if pd.isna(date_str) or date_str is None:
        return None
    date_str = str(date_str).strip()
    try:
        dt = pd.to_datetime(date_str, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date().isoformat()
    except Exception:
        return None

def normalize_title(title: str) -> str | None:
    if title is None or pd.isna(title):
        return None
    return " ".join(str(title).strip().lower().split())

# Compruba si el isbn13 es correcto y lo usa como ID (modelo canónico).
# Si no, genera un hash estable a partir de titulo_normalizado, autor_normalizado, editorial_normalizada, anio_publicacion.
def generate_book_id_from_row(row: pd.Series) -> str:
    isbn13 = normalize_isbn13(row.get("isbn13"))
    if isbn13 is not None:
        return isbn13

    def safe_str(v) -> str:
        if v is None or pd.isna(v):
            return ""
        return str(v).strip().lower()

    key = "|".join(
        [
            safe_str(row.get("titulo_normalizado")),
            safe_str(row.get("autor_normalizado")),
            safe_str(row.get("editorial_normalizada")),
            safe_str(row.get("anio_publicacion")),
        ]
    )

    return hashlib.sha1(key.encode("utf-8")).hexdigest()

# ------------------------------------------------------------
# Validadores (para métricas y "soft fail")
# ------------------------------------------------------------

# Aproximación a BCP-47: idioma[-region[-variant...]].
def idioma_valido(x) -> bool:
    if not isinstance(x, str):
        return False
    x = x.strip()
    if not x:
        return False
    parts = x.split("-")
    # primera parte: 2-3 letras
    if not (2 <= len(parts[0]) <= 3 and parts[0].isalpha()):
        return False
    # resto: alfanumérico 1-8 chars (permitimos '_' dentro)
    for p in parts[1:]:
        p_clean = p.replace("_", "")
        if not (1 <= len(p_clean) <= 8 and p_clean.isalnum()):
            return False
    return True

# Aproximación a ISO-4217: 3 letras mayúsculas.
def moneda_valida(x) -> bool:    
    if not isinstance(x, str):
        return False
    x = x.strip()
    return len(x) == 3 and x.isalpha() and x.upper() == x

# ------------------------------------------------------------
# Construcción staging
# ------------------------------------------------------------

def build_staging(df_gr: pd.DataFrame, df_gb: pd.DataFrame) -> pd.DataFrame:
    # Goodreads
    df_gr = df_gr.copy()
    df_gr["source_name"] = "goodreads"
    df_gr["source_file"] = "goodreads_books.json"
    df_gr["row_number"] = df_gr.index + 1

    # limpia ISBN
    df_gr["isbn10"] = df_gr["isbn10"].apply(clean_isbn)
    #df_gr["isbn13"] = df_gr["isbn13"].apply(clean_isbn)
    df_gr["isbn13"] = df_gr["isbn13"].apply(normalize_isbn13)

    # Google Books
    df_gb = df_gb.copy()
    df_gb["source_name"] = "googlebooks"
    df_gb["source_file"] = "googlebooks_books.csv"
    df_gb["row_number"] = df_gb.index + 1

    # limpia ISBN
    df_gb["isbn10"] = df_gb["isbn10"].apply(clean_isbn)
    #df_gb["isbn13"] = df_gb["isbn13"].apply(clean_isbn)
    df_gb["isbn13"] = df_gb["isbn13"].apply(normalize_isbn13)

    # Intentar derivar isbn13 desde isbn10 si falta
    mask_missing_isbn13 = df_gb["isbn13"].isna() & df_gb["isbn10"].notna()
    df_gb.loc[mask_missing_isbn13, "isbn13"] = df_gb.loc[mask_missing_isbn13, "isbn10"].apply(to_isbn13)

    # Opcional para asegurar tipo string
    #df_gr["isbn13"] = df_gr["isbn13"].astype("string")
    #df_gb["isbn13"] = df_gb["isbn13"].astype("string")

    # Columnas comunes mínimas para staging
    gr_cols = {
        "title": "titulo",
        "author": "autor_principal",
        "rating": "rating",
        "ratings_count": "ratings_count",
        "book_url": "book_url",
        "isbn10": "isbn10",
        "isbn13": "isbn13",
        "asin": "asin",
    }
    df_gr_common = df_gr[list(gr_cols.keys()) + ["source_name", "source_file", "row_number"]].rename(columns=gr_cols)

    gb_cols = {
        "title": "titulo",
        "authors": "autores",
        "publisher": "editorial",
        "pub_date": "fecha_publicacion_raw",
        "language": "idioma_raw",
        "categories": "categorias",
        "isbn10": "isbn10",
        "isbn13": "isbn13",
        "asin": "asin",
        "price_amount": "precio",
        "price_currency": "moneda",
    }

    # Asegurar columnas aunque no existan en df_gb
    for col in gb_cols.keys():
        if col not in df_gb.columns:
            df_gb[col] = np.nan

    df_gb_common = df_gb[list(gb_cols.keys()) + ["source_name", "source_file", "row_number"]].rename(columns=gb_cols)

    # Campos que Goodreads no tiene, los añadimos vacíos
    for col in ["autores", "editorial", "fecha_publicacion_raw", "idioma_raw", "categorias", "precio", "moneda"]:
        if col not in df_gr_common.columns:
            df_gr_common[col] = np.nan

    # Google no tiene rating/ratings_count/book_url
    for col in ["rating", "ratings_count", "book_url"]:
        if col not in df_gb_common.columns:
            df_gb_common[col] = np.nan

    staging = pd.concat([df_gr_common, df_gb_common], ignore_index=True)

    # Normalización
    staging["titulo_normalizado"] = staging["titulo"].apply(normalize_title)
    staging["idioma"] = staging["idioma_raw"].apply(normalize_language)
    staging["fecha_publicacion"] = staging["fecha_publicacion_raw"].apply(normalize_date)
    staging["moneda"] = staging["moneda"].apply(normalize_currency)

    # Autores y categorías como listas (se asume separador "|")
    staging["autores"] = staging["autores"].fillna("")
    staging["categorias"] = staging["categorias"].fillna("")

    staging["autores_list"] = staging["autores"].apply(lambda x: [a.strip() for a in str(x).split("|") if a.strip()])
    staging["categorias_list"] = staging["categorias"].apply(lambda x: [c.strip() for c in str(x).split("|") if c.strip()])

    # autor_principal: si falta, coger el primer autor de la lista
    if "autor_principal" not in staging.columns:
        staging["autor_principal"] = np.nan

    staging["autor_principal"] = staging["autor_principal"].where(
        staging["autor_principal"].notna(),
        staging["autores_list"].apply(lambda lst: lst[0] if lst else None),
    )

    # Año de publicación derivado de fecha_publicacion
    staging["anio_publicacion"] = staging["fecha_publicacion"].apply(
        lambda x: int(str(x)[:4]) if pd.notna(x) else None
    )

    # Enriquecimiento ligero: longitud del título
    staging["longitud_titulo"] = staging["titulo"].fillna("").apply(lambda x: len(str(x)))

    return staging

# ------------------------------------------------------------
# Anotar errores (soft fail) en staging
# ------------------------------------------------------------

def annotate_errors(staging: pd.DataFrame) -> pd.DataFrame:
    """
    Marca registros con errores de calidad.
    Añade columnas:
      - error_codes: lista de códigos de regla que fallan
      - has_error: True si hay algún error
    Reglas de ejemplo:
      R1_MISSING_KEY_TITULO_AUTOR
      R2_INVALID_DATE
      R3_INVALID_LANGUAGE
      R4_INVALID_CURRENCY
      R5_INVALID_RATING
    """

    def compute_row_errors(row: pd.Series) -> List[str]:
        codes: List[str] = []

        # R1: clave mínima título + autor_principal
        titulo = row.get("titulo")
        autor_principal = row.get("autor_principal")
        if (pd.isna(titulo) or str(titulo).strip() == "") or (pd.isna(autor_principal) or str(autor_principal).strip() == ""):
            codes.append("R1_MISSING_KEY_TITULO_AUTOR")

        # R2: fecha inválida (había valor raw pero no se pudo parsear)
        date_raw = row.get("fecha_publicacion_raw")
        date_norm = row.get("fecha_publicacion")
        if date_raw is not None and not pd.isna(date_raw) and str(date_raw).strip() != "":
            if date_norm is None or pd.isna(date_norm):
                codes.append("R2_INVALID_DATE")

        # R3: idioma inválido (si no es nulo)
        idioma_val = row.get("idioma")
        if idioma_val is not None and not pd.isna(idioma_val):
            if not idioma_valido(idioma_val):
                codes.append("R3_INVALID_LANGUAGE")

        # R4: moneda inválida (si no es nula)
        moneda_val = row.get("moneda")
        if moneda_val is not None and not pd.isna(moneda_val):
            if not moneda_valida(moneda_val):
                codes.append("R4_INVALID_CURRENCY")

        # R5: rating fuera de rango o no numérico (si no es nulo)
        rating = row.get("rating")
        if rating is not None and not pd.isna(rating):
            if not (isinstance(rating, (int, float)) and 0 <= rating <= 5):
                codes.append("R5_INVALID_RATING")

        return codes

    staging = staging.copy()
    staging["error_codes"] = staging.apply(compute_row_errors, axis=1)
    staging["has_error"] = staging["error_codes"].apply(lambda codes: len(codes) > 0)
    return staging

# ------------------------------------------------------------
# Deduplicación & dim_book
# ------------------------------------------------------------

def deduplicate(staging: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Deduplicar por book_id usando reglas de supervivencia:
      - ID preferente: isbn13.
      - En ausencia de isbn13: hash de (titulo_normalizado, autor_normalizado, editorial_normalizada, anio_publicacion).
      - Fila ganadora por book_id (SOLO REGISTROS VÁLIDOS):
          * tiene isbn13
          * tiene precio
          * prioridad de fuente (googlebooks > goodreads)
          * título más completo (longitud_titulo mayor)
      - Unión de autores y categorías sin duplicados (sobre todas las filas válidas del mismo book_id).
      - Registros con errores se marcan en book_source_detail pero NO se incluyen en dim_book.
    """

    staging = staging.copy()

    # 1. Asegurar columnas normalizadas
    
    # titulo_normalizado ya viene de build_staging, pero nos aseguramos
    if "titulo_normalizado" not in staging.columns:
        staging["titulo_normalizado"] = (
            staging.get("titulo", "")
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
        )

    # autor_normalizado
    if "autor_normalizado" not in staging.columns:
        base_col = None
        for c in ["autor_principal", "autor", "author"]:
            if c in staging.columns:
                base_col = c
                break

        if base_col is not None:
            staging["autor_normalizado"] = (staging[base_col].fillna("").astype(str).str.strip().str.lower())
        else:
            staging["autor_normalizado"] = ""

    # editorial_normalizada
    if "editorial_normalizada" not in staging.columns:
        base_col = None
        for c in ["editorial", "publisher"]:
            if c in staging.columns:
                base_col = c
                break

        if base_col is not None:
            staging["editorial_normalizada"] = (staging[base_col].fillna("").astype(str).str.strip().str.lower())
        else:
            staging["editorial_normalizada"] = ""

    # 2. Generar book_id
    staging["book_id"] = staging.apply(generate_book_id_from_row, axis=1)

    # 3. Flags y prioridad de fuente
    staging["has_isbn13"] = staging["isbn13"].notna()
    staging["has_precio"] = staging["precio"].notna()

    prioridad_fuente = {"googlebooks": 3, "goodreads": 2}
    staging["prioridad_fuente"] = staging["source_name"].map(prioridad_fuente).fillna(1)

    # 4. Anotar errores (soft fail)
    staging = annotate_errors(staging)

    # Solo los registros válidos participan en la deduplicación de dim_book
    staging_valid = staging[~staging["has_error"]].copy()

    # Orden para elegir ganador (solo válidos)
    staging_valid_sorted = staging_valid.sort_values(
        by=["book_id", "has_isbn13", "has_precio", "prioridad_fuente", "longitud_titulo"],
        ascending=[True, False, False, False, False],
    )

    winners = staging_valid_sorted.groupby("book_id", as_index=False).first()

    # 5. Unión de autores/categorías sin duplicados (solo válidos)
    autores_agg = (
        staging_valid.groupby("book_id")["autores_list"]
        .apply(lambda lists_: sorted({a for sub in lists_ for a in sub}))
        .rename("autores_unificados")
    )

    categorias_agg = (
        staging_valid.groupby("book_id")["categorias_list"]
        .apply(lambda lists_: sorted({c for sub in lists_ for c in sub}))
        .rename("categorias_unificadas")
    )

    winners = winners.merge(autores_agg, on="book_id", how="left")
    winners = winners.merge(categorias_agg, on="book_id", how="left")

    # 6. Construir dim_book (solo con winners válidos)
    dim_book = pd.DataFrame()
    dim_book["book_id"] = winners["book_id"]

    # título
    if "titulo" in winners.columns:
        dim_book["titulo"] = winners["titulo"]
    elif "title" in winners.columns:
        dim_book["titulo"] = winners["title"]
    else:
        dim_book["titulo"] = ""

    dim_book["titulo_normalizado"] = winners["titulo_normalizado"]

    # autor_principal
    if "autor_principal" in winners.columns:
        dim_book["autor_principal"] = winners["autor_principal"]
    elif "autor" in winners.columns:
        dim_book["autor_principal"] = winners["autor"]
    elif "author" in winners.columns:
        dim_book["autor_principal"] = winners["author"]
    else:
        dim_book["autor_principal"] = ""

    # autores (unificados)
    dim_book["autores"] = winners["autores_unificados"].apply(
        lambda x: x if isinstance(x, list) and len(x) > 0 else []
    )

    # editorial
    if "editorial" in winners.columns:
        dim_book["editorial"] = winners["editorial"]
    elif "publisher" in winners.columns:
        dim_book["editorial"] = winners["publisher"]
    else:
        dim_book["editorial"] = None

    # fechas / año
    dim_book["anio_publicacion"] = winners.get("anio_publicacion")
    dim_book["fecha_publicacion"] = winners.get("fecha_publicacion")

    # idioma / isbn
    dim_book["idioma"] = winners.get("idioma")
    dim_book["isbn10"] = winners.get("isbn10")
    dim_book["isbn13"] = winners.get("isbn13")
    dim_book["asin"] = winners.get("asin")
    
    # opcionales
    dim_book["paginas"] = winners.get("paginas", np.nan)
    dim_book["formato"] = winners.get("formato", None)

    # categorías (unificadas)
    dim_book["categorias"] = winners["categorias_unificadas"].apply(
        lambda x: x if isinstance(x, list) and len(x) > 0 else []
    )

    # precio / moneda / fuente
    dim_book["precio"] = winners.get("precio")
    dim_book["moneda"] = winners.get("moneda")
    dim_book["fuente_ganadora"] = winners.get("source_name")
    dim_book["ts_ultima_act"] = datetime.now(UTC).isoformat()

    # 7. book_source_detail con TODOS los registros (válidos + con error)
    # Orden similar al de deduplicación, pero incluyendo los inválidos
    staging_sorted_full = staging.sort_values(
        by=["book_id", "has_isbn13", "has_precio", "prioridad_fuente", "longitud_titulo"],
        ascending=[True, False, False, False, False],
    )

    book_source_detail = staging_sorted_full.copy()
    book_source_detail.reset_index(drop=True, inplace=True)
    book_source_detail["source_id"] = book_source_detail.index + 1
    book_source_detail["row_number"] = book_source_detail.groupby("source_name").cumcount() + 1
    book_source_detail["book_id_candidato"] = book_source_detail["book_id"]
    book_source_detail["ts_ingesta"] = datetime.now(UTC).isoformat()

    return dim_book, book_source_detail


# --------------------------
# Métricas de calidad
# --------------------------

def compute_quality_metrics(dim_book: pd.DataFrame, book_source_detail: pd.DataFrame) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}

    # Métricas sobre dim_book
    metrics["dim_book"] = {
        **compute_basic_counts(dim_book),
        "nulos_por_campo": compute_null_percentages(dim_book),
    }

    # Métricas sobre book_source_detail
    # Asegurar que las columnas usadas en duplicados existen (por si acaso)
    for col in ["titulo_normalizado", "autor_principal", "editorial"]:
        if col not in book_source_detail.columns:
            book_source_detail[col] = None

    # Asegurar también 'titulo' para validaciones de clave
    if "titulo" not in book_source_detail.columns:
        book_source_detail["titulo"] = None

    metrics["book_source_detail"] = {
        **compute_basic_counts(book_source_detail),
        "nulos_por_campo": compute_null_percentages(book_source_detail),
        "duplicados_por_isbn13": count_duplicates(book_source_detail, subset=["isbn13"]),
        "duplicados_por_titulo_autor_editorial": count_duplicates(
            book_source_detail, subset=["titulo_normalizado", "autor_principal", "editorial"]
        ),
    }

    # Filas por fuente (conteos)
    if "source_name" in book_source_detail.columns:
        filas_por_fuente = (
            book_source_detail["source_name"]
            .value_counts(dropna=False)
            .to_dict()
        )
    else:
        filas_por_fuente = {}

    metrics["book_source_detail"]["filas_por_fuente"] = filas_por_fuente

    # --------------------------
    # Validaciones
    # --------------------------

    # % fechas válidas = porcentaje de filas con fecha_publicacion no nula
    if "fecha_publicacion" in book_source_detail.columns:
        pct_fechas_validas = float(book_source_detail["fecha_publicacion"].notna().mean())
    else:
        pct_fechas_validas = 0.0

    validaciones: Dict[str, Any] = {
        "porcentaje_idiomas_validos": float(book_source_detail["idioma"].apply(idioma_valido).mean())
        if "idioma" in book_source_detail.columns
        else 0.0,
        "porcentaje_monedas_validas": float(book_source_detail["moneda"].apply(moneda_valida).mean())
        if "moneda" in book_source_detail.columns
        else 0.0,
        "porcentaje_fechas_validas": pct_fechas_validas,
    }

    # Clave requerida: al menos titulo y autor_principal presentes
    validaciones["porcentaje_clave_titulo_autor_presente"] = float(
        (
            book_source_detail["titulo"].notna() & book_source_detail["autor_principal"].notna()
        ).mean()
    )

    # Definimos "porcentaje_filas_validas" como esa misma clave presente
    validaciones["porcentaje_filas_validas"] = validaciones["porcentaje_clave_titulo_autor_presente"]

    # Rango simple para rating (si la columna existe)
    if "rating" in book_source_detail.columns:
        validaciones["porcentaje_ratings_validos"] = float(
            book_source_detail["rating"].apply(
                lambda x: pd.isna(x)
                or (isinstance(x, (int, float)) and 0 <= x <= 5)
            ).mean()
        )

    # Nulos “clave” destacados
    for col in ["titulo", "isbn13", "precio"]:
        if col in book_source_detail.columns:
            key = f"porcentaje_nulos_{col}"
            validaciones[key] = float(book_source_detail[col].isna().mean())

    # % registros marcados con error (soft fail)
    if "has_error" in book_source_detail.columns:
        validaciones["porcentaje_registros_invalidos"] = float(book_source_detail["has_error"].mean())

    metrics["validaciones"] = validaciones

    # --------------------------
    # Logs por archivo y por regla (trazabilidad)
    # --------------------------
    logs_por_archivo: Dict[str, Dict[str, int]] = {}
    logs_por_regla: Dict[str, int] = {}

    if "error_codes" in book_source_detail.columns and "source_file" in book_source_detail.columns:
        for _, row in book_source_detail[["source_file", "error_codes"]].iterrows():
            src = row["source_file"] if pd.notna(row["source_file"]) else "UNKNOWN"
            codes = row["error_codes"]
            if not isinstance(codes, list):
                continue
            for code in codes:
                logs_por_archivo.setdefault(src, {}).setdefault(code, 0)
                logs_por_archivo[src][code] += 1
                logs_por_regla.setdefault(code, 0)
                logs_por_regla[code] += 1

    metrics["logs"] = {
        "por_archivo": logs_por_archivo,
        "por_regla": logs_por_regla,
    }

    return metrics


# --------------------------
# Generación schema.md
# --------------------------

def write_schema(dim_book: pd.DataFrame, book_source_detail: pd.DataFrame) -> None:
    """
    Genera docs/schema.md con un esquema más rico de ambas tablas:
      - nombre, tipo, nullability, ejemplo
      - breve descripción/reglas globales de deduplicación y supervivencia
    """
    lines: List[str] = []
    lines.append("# Esquema de tablas\n\n")

    def describe_df(name: str, df: pd.DataFrame):
        lines.append(f"## {name}\n\n")
        lines.append("| columna | tipo | nullability | ejemplo |\n")
        lines.append("|---------|------|-------------|---------|\n")

        nulls = compute_null_percentages(df)
        for col, dtype in df.dtypes.items():
            pct_null = nulls.get(col, 0.0)
            nullability = "NOT NULL" if pct_null == 0 else f"NULL ({pct_null:.1%})"
            # ejemplo: primer valor no nulo
            non_null_series = df[col].dropna()
            if not non_null_series.empty:
                ejemplo_val = str(non_null_series.iloc[0])
                if len(ejemplo_val) > 40:
                    ejemplo_val = ejemplo_val[:37] + "..."
            else:
                ejemplo_val = ""
            lines.append(f"| {col} | {dtype} | {nullability} | {ejemplo_val} |\n")
        lines.append("\n")

    describe_df("dim_book", dim_book)
    describe_df("book_source_detail", book_source_detail)

    os.makedirs("docs", exist_ok=True)
    with open("docs/schema.md", "w", encoding="utf-8") as f:
        f.writelines(lines)


def main():
    os.makedirs("standard", exist_ok=True)
    os.makedirs("docs", exist_ok=True)
    os.makedirs("staging", exist_ok=True)  # temporales fuera de landing/

    df_gr, df_gb = load_sources()
    staging = build_staging(df_gr, df_gb)

    # Guardar staging como artefacto temporal (no obligatorio, pero útil)
    staging.to_parquet("staging/books_staging.parquet", index=False)

    dim_book, book_source_detail = deduplicate(staging)
    metrics = compute_quality_metrics(dim_book, book_source_detail)

    # Metadatos de entrada (filas/columnas/tamaño por fuente)
    metrics["entradas"] = {
        "goodreads": {
            "ruta": "landing/goodreads_books.json",
            "n_filas": int(df_gr.shape[0]),
            "n_columnas": int(df_gr.shape[1]),
            "tamano_bytes": int(os.path.getsize("landing/goodreads_books.json")),
        },
        "googlebooks": {
            "ruta": "landing/googlebooks_books.csv",
            "n_filas": int(df_gb.shape[0]),
            "n_columnas": int(df_gb.shape[1]),
            "tamano_bytes": int(os.path.getsize("landing/googlebooks_books.csv")),
        },
    }

    # Guardar Parquet (standard/)
    dim_book.to_parquet("standard/dim_book.parquet", index=False)
    book_source_detail.to_parquet("standard/book_source_detail.parquet", index=False)

    # Guardar quality_metrics.json
    with open("docs/quality_metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    # Guardar schema.md
    write_schema(dim_book, book_source_detail)

    print("Pipeline de integración completado.")
    print("standard/dim_book.parquet")
    print("standard/book_source_detail.parquet")
    print("staging/books_staging.parquet")
    print("docs/quality_metrics.json")
    print("docs/schema.md")

    # Convertir Parquet → CSV
    os.makedirs("parquet_a_csv", exist_ok=True)
    parquet_files = [
        "standard/dim_book.parquet",
        "standard/book_source_detail.parquet",
        "staging/books_staging.parquet",
    ]

    for pq_file in parquet_files:
        df = pd.read_parquet(pq_file)
        csv_name = os.path.splitext(os.path.basename(pq_file))[0] + ".csv"
        csv_path = os.path.join("parquet_a_csv", csv_name)
        df.to_csv(csv_path, index=False)
        print(f"Convertido a CSV: {csv_path}")

if __name__ == "__main__":
    main()
