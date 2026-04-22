from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

from utils.constants import SUPABASE_DEBUG_INFO, SUPABASE_KEY, SUPABASE_URL
from utils.data_helpers import normalize_columns


def _supabase_config_ready() -> bool:
    return bool(SUPABASE_URL and SUPABASE_KEY)


def _notify_missing_config_once() -> None:
    if st.session_state.get("_supabase_config_warned"):
        return
    st.session_state["_supabase_config_warned"] = True
    st.error(
        "Thiếu cấu hình Supabase. Vui lòng set SUPABASE_URL và SUPABASE_KEY "
        "trong Streamlit Secrets hoặc Environment Variables."
    )
    st.caption(
        "Debug config: "
        f"url_loaded={SUPABASE_DEBUG_INFO.get('url_loaded')} "
        f"(source={SUPABASE_DEBUG_INFO.get('url_source')}), "
        f"key_loaded={SUPABASE_DEBUG_INFO.get('key_loaded')} "
        f"(source={SUPABASE_DEBUG_INFO.get('key_source')}), "
        f"key_length={SUPABASE_DEBUG_INFO.get('key_length')}"
    )


@st.cache_resource
def get_http_session() -> requests.Session:
    if not _supabase_config_ready():
        _notify_missing_config_once()
    session = requests.Session()
    session.headers.update(
        {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
        }
    )
    return session


def _response_is_null_id_error(resp: requests.Response) -> bool:
    try:
        payload = resp.json() if hasattr(resp, "json") else {}
    except Exception:
        payload = {}
    code = str(payload.get("code", ""))
    message = str(payload.get("message", ""))
    details = str(payload.get("details", ""))
    raw_text = str(getattr(resp, "text", ""))
    text_join = f"{message} {details} {raw_text}".lower()
    return code == "23502" and "null value" in text_join and 'column "id"' in text_join


def _normalize_row_for_write(item: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(item)
    if "ID" in row and "id" not in row:
        row["id"] = row.pop("ID")
    if "id" in row and row["id"] is not None and str(row["id"]).strip() != "":
        try:
            row["id"] = int(float(row["id"]))
        except Exception:
            pass
    return row


def _get_next_id(table_name: str) -> int:
    session = get_http_session()
    try:
        resp = session.get(
            f"{SUPABASE_URL}{table_name}",
            params={"select": "id", "order": "id.desc", "limit": 1},
            timeout=15,
        )
        if resp.status_code in (200, 206):
            data = resp.json()
            if isinstance(data, list) and data:
                last_id = data[0].get("id")
                if last_id is not None and str(last_id).strip() != "":
                    return int(float(last_id)) + 1
    except Exception:
        pass
    return 1


@st.cache_data(ttl=60, show_spinner=False)
def lay_du_lieu_supabase(table_name: str, query_params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    if not _supabase_config_ready():
        _notify_missing_config_once()
        return pd.DataFrame()
    params = dict(query_params or {})
    params.setdefault("order", "id.asc")
    session = get_http_session()
    try:
        response = session.get(f"{SUPABASE_URL}{table_name}", params=params, timeout=20)
        if response.status_code not in (200, 206):
            return pd.DataFrame()
        data = response.json()
        if not data or (isinstance(data, dict) and "error" in data):
            return pd.DataFrame()
        df = pd.DataFrame(data)
        return normalize_columns(df)
    except Exception:
        return pd.DataFrame()


def invalidate_data_cache() -> None:
    lay_du_lieu_supabase.clear()


def ghi_du_lieu_supabase(table_name: str, list_data: List[Dict[str, Any]]) -> bool:
    session = get_http_session()
    try:
        clean_data = [_normalize_row_for_write(item) for item in list_data]
        rows_update = [r for r in clean_data if ("id" in r and r["id"] is not None and str(r["id"]).strip() != "")]
        rows_insert = [r for r in clean_data if not ("id" in r and r["id"] is not None and str(r["id"]).strip() != "")]

        if rows_insert:
            insert_headers = {"Prefer": "return=minimal"}
            insert_url = f"{SUPABASE_URL}{table_name}"
            response_insert = session.post(insert_url, json=rows_insert, headers=insert_headers, timeout=20)
            if response_insert.status_code not in (200, 201, 204):
                if _response_is_null_id_error(response_insert):
                    next_id = _get_next_id(table_name)
                    rows_insert_with_id = []
                    for row in rows_insert:
                        new_row = row.copy()
                        new_row["id"] = next_id
                        next_id += 1
                        rows_insert_with_id.append(new_row)
                    response_retry = session.post(
                        insert_url, json=rows_insert_with_id, headers=insert_headers, timeout=20
                    )
                    if response_retry.status_code not in (200, 201, 204):
                        st.error(f"Lỗi insert ({table_name}) sau khi tự cấp id: {response_retry.text}")
                        return False
                else:
                    st.error(f"Lỗi insert ({table_name}): {response_insert.text}")
                    return False

        if rows_update:
            update_headers = {"Prefer": "return=minimal"}
            for row in rows_update:
                row_id = row["id"]
                payload = {k: v for k, v in row.items() if k != "id"}
                if not payload:
                    continue
                update_url = f"{SUPABASE_URL}{table_name}?id=eq.{row_id}"
                response_update = session.patch(update_url, json=payload, headers=update_headers, timeout=20)
                if response_update.status_code not in (200, 201, 204):
                    st.error(f"Lỗi update id={row_id} ({table_name}): {response_update.text}")
                    return False

        invalidate_data_cache()
        return True
    except Exception as exc:
        st.error(f"Lỗi kết nối: {exc}")
        return False


def xoa_dong_supabase(table_name: str, row_id: int) -> bool:
    session = get_http_session()
    try:
        response = session.delete(f"{SUPABASE_URL}{table_name}?id=eq.{row_id}", timeout=20)
        if response.status_code in (200, 204):
            invalidate_data_cache()
            return True
        st.error(f"Lỗi xóa ({table_name}): {response.text}")
        return False
    except Exception as exc:
        st.error(f"Không thể kết nối để xóa: {exc}")
        return False


def lay_log_tien_theo_thang(thang_nam: str) -> pd.DataFrame:
    return lay_du_lieu_supabase("tienca_log", query_params={"THÁNG": thang_nam})


def get_user_by_username(username: str) -> Tuple[Optional[Dict[str, Any]], str]:
    user_login = str(username or "").strip().lower()
    if not user_login:
        return None, "Vui lòng nhập tên đăng nhập."
    df_user = lay_du_lieu_supabase("nhansu_2026")
    if df_user.empty:
        return None, "Không tìm thấy dữ liệu nhân sự trên hệ thống!"
    match = df_user[
        (df_user["USERNAME"].astype(str).str.lower() == user_login)
        & (df_user["TRẠNG THÁI"].astype(str).str.upper() != "NGHỈ LÀM")
    ]
    if match.empty:
        return None, "Không tìm thấy tài khoản hợp lệ."
    return match.iloc[0].to_dict(), ""


def update_user_password(username: str, current_password: str, new_password: str) -> Tuple[bool, str]:
    user_row, err = get_user_by_username(username)
    if user_row is None:
        return False, err
    if str(user_row.get("PASSWORD", "")) != str(current_password):
        return False, "Mật khẩu hiện tại không đúng."
    row_id = user_row.get("id", user_row.get("ID"))
    if row_id is None or str(row_id).strip() == "":
        return False, "Không xác định được ID người dùng để cập nhật."
    ok = ghi_du_lieu_supabase("nhansu_2026", [{"id": int(float(row_id)), "PASSWORD": str(new_password)}])
    if not ok:
        return False, "Không thể cập nhật mật khẩu lên Supabase."
    return True, "Đổi mật khẩu thành công."


def log_tools_sent_for_sterilization(rows: List[Dict[str, Any]]) -> bool:
    if not rows:
        return True
    return ghi_du_lieu_supabase("kho_gui_hap_log", rows)


def log_tools_received_with_expiry(rows: List[Dict[str, Any]]) -> bool:
    if not rows:
        return True
    return ghi_du_lieu_supabase("kho_nhan_ve_log", rows)
