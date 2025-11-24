# src/utils_quality.py

from typing import Dict, Any
import pandas as pd
import numpy as np


def compute_null_percentages(df: pd.DataFrame) -> Dict[str, float]:
    return {col: float(df[col].isna().mean()) for col in df.columns}


def compute_basic_counts(df: pd.DataFrame) -> Dict[str, Any]:
    return {
        "rows": int(len(df)),
        "columns": int(df.shape[1]),
    }


def count_duplicates(df: pd.DataFrame, subset: list) -> int:
    return int(df.duplicated(subset=subset, keep=False).sum())
