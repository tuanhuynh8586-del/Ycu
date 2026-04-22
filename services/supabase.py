from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

from utils.constants import SUPABASE_DEBUG_INFO, SUPABASE_KEY, SUPABASE_URL
from utils.data_helpers import normalize_columns

# Ghi chú: dự án đã dùng PostgREST (requests) qua `SUPABASE_URL`/`SUPABASE_KEY`.
# Tránh khởi tạo supabase-py client ở mức module (dễ lỗi khi env/secret thiếu).

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
                # Retry once for schema/case mismatch (PGRST204: missing column in schema cache)
                if _is_missing_column_error(response_insert):
                    lowered_rows: List[Dict[str, Any]] = []
                    for row in rows_insert:
                        lowered: Dict[str, Any] = {}
                        for k, v in row.items():
                            # Keep "id" as is, lower everything else (common when DB columns are unquoted lowercase).
                            if str(k) == "id":
                                lowered[k] = v
                            else:
                                lowered[str(k).strip().lower()] = v
                        lowered_rows.append(lowered)
                    retry_resp = session.post(insert_url, json=lowered_rows, headers=insert_headers, timeout=20)
                    if retry_resp.status_code in (200, 201, 204):
                        invalidate_data_cache()
                        return True
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


def _is_missing_column_error(resp: requests.Response) -> bool:
    try:
        payload = resp.json() if hasattr(resp, "json") else {}
    except Exception:
        payload = {}
    code = str(payload.get("code", ""))
    message = str(payload.get("message", ""))
    return code == "PGRST204" and "could not find the" in message.lower()


def _try_insert_variants(table_name: str, rows_variants: List[List[Dict[str, Any]]]) -> bool:
    session = get_http_session()
    url = f"{SUPABASE_URL}{table_name}"
    headers = {"Prefer": "return=minimal"}
    last_error_text = ""
    for rows in rows_variants:
        try:
            resp = session.post(url, json=[_normalize_row_for_write(r) for r in rows], headers=headers, timeout=20)
            if resp.status_code in (200, 201, 204):
                invalidate_data_cache()
                return True
            last_error_text = getattr(resp, "text", "") or ""
            # Nếu lỗi do thiếu cột (schema/case mismatch), thử biến thể khác.
            if _is_missing_column_error(resp):
                continue
        except Exception as exc:
            last_error_text = str(exc)
        break
    if last_error_text:
        st.error(f"Lỗi insert ({table_name}): {last_error_text}")
    return False


def log_tools_received_with_expiry(rows: List[Dict[str, Any]]) -> bool:
    if not rows:
        return True
    # Bảng `kho_nhan_ve_log` hay bị lệch kiểu đặt tên cột (HOA vs thường, hoặc VN).
    # Retry theo các biến thể phổ biến để tránh lỗi PGRST204 (schema cache/column mismatch).
    variants: List[List[Dict[str, Any]]] = []

    # 1) Lowercase snake_case (phổ biến nhất nếu schema tạo không quote).
    v_lower: List[Dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        mapping = {
            "TOOL_NAME": "tool_name",
            "QUANTITY": "quantity",
            "REMAINING_QTY": "remaining_qty",
            "DATE_RECEIVED": "date_received",
            "DATE_RECEIVED_DATE": "date_received_date",
            "EXPIRY_DATE": "expiry_date",
            "EXPIRY_DATE_DATE": "expiry_date_date",
        }
        for src, dst in mapping.items():
            if src in rr and dst not in rr:
                rr[dst] = rr.pop(src)
        v_lower.append(rr)
    variants.append(v_lower)

    # 2) Giữ nguyên payload hiện tại (schema dùng cột HOA/quoted).
    variants.append(rows)

    # 3) Vietnamese columns (một số kho dùng cột VN).
    v_vn: List[Dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        mapping = {
            "TOOL_NAME": "TÊN BỘ DỤNG CỤ",
            "QUANTITY": "SỐ LƯỢNG",
            "REMAINING_QTY": "SL CÒN",
            "DATE_RECEIVED": "NGÀY NHẬN",
            "DATE_RECEIVED_DATE": "NGAY_NHAN_DATE",
            "EXPIRY_DATE": "HẠN DÙNG",
            "EXPIRY_DATE_DATE": "HAN_DUNG_DATE",
        }
        for src, dst in mapping.items():
            if src in rr and dst not in rr:
                rr[dst] = rr.pop(src)
        v_vn.append(rr)
    variants.append(v_vn)

    return _try_insert_variants("kho_nhan_ve_log", variants)


def get_fefo_batches(ten_dung_cu: str) -> pd.DataFrame:
    tool_name = str(ten_dung_cu or "").strip()
    if not tool_name:
        return pd.DataFrame()
    df = lay_du_lieu_supabase("kho_lo_hap")
    if df.empty:
        return df
    status_col = "TRANG_THAI" if "TRANG_THAI" in df.columns else "TRẠNG THÁI" if "TRẠNG THÁI" in df.columns else ""
    if status_col:
        df = df[df[status_col].astype(str).str.lower() == "ready"]
    name_col = "TEN_DUNG_CU" if "TEN_DUNG_CU" in df.columns else "TÊN DỤNG CỤ" if "TÊN DỤNG CỤ" in df.columns else ""
    if name_col:
        df = df[df[name_col].astype(str) == tool_name]
    if "HAN_DUNG_DATE" in df.columns:
        df["__exp"] = pd.to_datetime(df["HAN_DUNG_DATE"], errors="coerce")
        df = df.sort_values(by=["__exp", "id"], kind="stable", na_position="last")
    else:
        expiry_col = "HAN_DUNG" if "HAN_DUNG" in df.columns else "HẠN DÙNG" if "HẠN DÙNG" in df.columns else ""
        if expiry_col:
            df["__exp"] = pd.to_datetime(df[expiry_col], errors="coerce")
            df = df.sort_values(by=["__exp", "id"], kind="stable", na_position="last")
    df = df.drop(columns=["__exp"], errors="ignore")
    return df


def insert_batch(ten_dung_cu: str, ngay_hap: date, so_luong: int, han_dung: date) -> bool:
    ngay_hap_text = ngay_hap.strftime("%d/%m/%Y")
    han_dung_text = han_dung.strftime("%d/%m/%Y")
    payload_upper = [
        {
            "TEN_DUNG_CU": str(ten_dung_cu),
            "NGAY_HAP": ngay_hap_text,
            "NGAY_HAP_DATE": ngay_hap.isoformat(),
            "SO_LUONG": int(so_luong),
            "HAN_DUNG": han_dung_text,
            "HAN_DUNG_DATE": han_dung.isoformat(),
            "TRANG_THAI": "ready",
        }
    ]
    if ghi_du_lieu_supabase("kho_lo_hap", payload_upper):
        return True
    # Fallback for schemas created with unquoted lowercase columns.
    payload_lower = [
        {
            "ten_dung_cu": str(ten_dung_cu),
            "ngay_hap": ngay_hap_text,
            "ngay_hap_date": ngay_hap.isoformat(),
            "so_luong": int(so_luong),
            "han_dung": han_dung_text,
            "han_dung_date": han_dung.isoformat(),
            "trang_thai": "ready",
        }
    ]
    return ghi_du_lieu_supabase("kho_lo_hap", payload_lower)


def deduct_batch(batch_id: int, so_luong: int) -> bool:
    df = lay_du_lieu_supabase("kho_lo_hap")
    if df.empty:
        return False
    row = df[df["id"] == int(batch_id)]
    if row.empty:
        return False
    current_qty = int(pd.to_numeric(row.iloc[0].get("SO_LUONG", 0), errors="coerce"))
    new_qty = max(0, current_qty - int(so_luong))
    payload_upper: Dict[str, Any] = {"id": int(batch_id), "SO_LUONG": new_qty}
    if new_qty == 0:
        payload_upper["TRANG_THAI"] = "used"
    if ghi_du_lieu_supabase("kho_lo_hap", [payload_upper]):
        return True
    payload_lower: Dict[str, Any] = {"id": int(batch_id), "so_luong": new_qty}
    if new_qty == 0:
        payload_lower["trang_thai"] = "used"
    return ghi_du_lieu_supabase("kho_lo_hap", [payload_lower])


def log_usage(
    ten_dung_cu: str,
    ngay_hap: Optional[date],
    so_luong: int,
    nguoi_lay: str,
) -> bool:
    now_ts = datetime.now()
    payload = [
        {
            "ten_dung_cu": str(ten_dung_cu),
            "ngay_hap": ngay_hap.strftime("%Y-%m-%d") if ngay_hap else None,
            "ngay_hap_dat": ngay_hap.isoformat() if ngay_hap else None,
            "so_luong": int(so_luong),
            "nguoi_lay": str(nguoi_lay),
            "thoi_diem_xuat_ts": now_ts.isoformat(sep=" ", timespec="seconds"),
        }
    ]
    return ghi_du_lieu_supabase("kho_xuat_log", payload)
# =========================
# Remember Me functions
# =========================
def update_remember_token(username: str, token: str):
    """Cập nhật token remember me cho user"""
    if not _supabase_config_ready():
        _notify_missing_config_once()
        return False
    user_login = str(username or "").strip()
    if not user_login:
        return False
    session = get_http_session()
    token_val = str(token or "").strip()

    # Supabase/PostgREST có thể dùng schema cột viết HOA (quoted) hoặc thường (lowercase).
    # Vì vậy thử lần lượt các biến thể để đảm bảo ghi được.
    user_cols = ["USERNAME", "username"]
    token_cols = ["REMEMBER_TOKEN", "remember_token"]
    for u_col in user_cols:
        for t_col in token_cols:
            resp = session.patch(
                f"{SUPABASE_URL}nhansu_2026?{u_col}=eq.{user_login}",
                json={t_col: token_val},
                headers={"Prefer": "return=minimal"},
                timeout=20,
            )
            if resp.status_code in (200, 204):
                invalidate_data_cache()
                return True

    st.error("Không thể ghi remember token lên Supabase (không match được cột USERNAME/REMEMBER_TOKEN).")
    return False


def update_remember_token_by_id(user_id: Any, token: str) -> bool:
    """Cập nhật token remember me theo id (ổn định nhất)."""
    if not _supabase_config_ready():
        _notify_missing_config_once()
        return False
    if user_id is None or str(user_id).strip() == "":
        return False
    try:
        uid = int(float(user_id))
    except Exception:
        return False

    session = get_http_session()
    token_val = str(token or "").strip()
    for t_col in ("REMEMBER_TOKEN", "remember_token"):
        resp = session.patch(
            f"{SUPABASE_URL}nhansu_2026?id=eq.{uid}",
            json={t_col: token_val},
            headers={"Prefer": "return=minimal"},
            timeout=20,
        )
        if resp.status_code in (200, 204):
            invalidate_data_cache()
            return True
    st.error("Không thể ghi remember token theo id (kiểm tra quyền update/RLS và tên cột).")
    return False

def get_user_by_token(token: str):
    """Lấy thông tin user từ token"""
    t = str(token or "").strip()
    if not t:
        return None
    for col in ("REMEMBER_TOKEN", "remember_token"):
        df = lay_du_lieu_supabase(
            "nhansu_2026",
            query_params={
                "select": "*",
                col: f"eq.{t}",
                "limit": 1,
            },
        )
        if not df.empty:
            return df.iloc[0].to_dict()
    return None

