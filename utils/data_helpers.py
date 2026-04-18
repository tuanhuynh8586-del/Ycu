from typing import Any, Iterable, List, Optional

import pandas as pd


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df.columns = [
        str(c).strip().lower() if str(c).strip().lower() == "id" else str(c).strip().upper()
        for c in df.columns
    ]
    return df


def stable_sort_dataframe(
    df: pd.DataFrame,
    primary_columns: Optional[List[str]] = None,
    fallback_name_columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    if df.empty:
        return df
    result = df.copy()
    primary_columns = primary_columns or []
    fallback_name_columns = fallback_name_columns or []

    sort_cols: List[str] = [c for c in primary_columns if c in result.columns]
    if "ORDER_INDEX" in result.columns:
        sort_cols.append("ORDER_INDEX")
    if "id" in result.columns:
        sort_cols.append("id")
    for col in fallback_name_columns:
        if col in result.columns:
            sort_cols.append(col)

    if sort_cols:
        return result.sort_values(by=sort_cols, kind="stable", na_position="last")
    return result


def get_fixed_order_list(
    values: Iterable[Any],
    reference_order: Optional[List[str]] = None,
) -> List[str]:
    clean_values = [str(v) for v in values if str(v).strip() != ""]
    if reference_order:
        order_map = {name: i for i, name in enumerate(reference_order)}
        return sorted(clean_values, key=lambda x: (order_map.get(x, 10**9), x))
    return clean_values
