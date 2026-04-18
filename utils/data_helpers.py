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

    # Ưu tiên các cột thứ tự tường minh để giữ fixed order sau khi update.
    order_candidates = [
        "ORDER_INDEX",
        "THỨ TỰ",
        "THU TU",
        "STT",
        "SỐ THỨ TỰ",
        "SO THU TU",
    ]
    temp_numeric_sort_cols: List[str] = []
    for idx, col in enumerate(order_candidates):
        if col in result.columns:
            temp_col = f"__sort_order_{idx}"
            result[temp_col] = pd.to_numeric(result[col], errors="coerce")
            sort_cols.append(temp_col)
            temp_numeric_sort_cols.append(temp_col)

    if "id" in result.columns:
        sort_cols.append("id")
    for col in fallback_name_columns:
        if col in result.columns:
            sort_cols.append(col)

    if sort_cols:
        sorted_df = result.sort_values(by=sort_cols, kind="stable", na_position="last")
        if temp_numeric_sort_cols:
            sorted_df = sorted_df.drop(columns=temp_numeric_sort_cols, errors="ignore")
        return sorted_df
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
