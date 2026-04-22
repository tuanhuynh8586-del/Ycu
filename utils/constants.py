import os
from pathlib import Path

import streamlit as st

MENU_ITEMS = (
    "Nhân sự & Đăng ký Off",
    "Phân phòng trực",
    "Tiền ca & Điều phối",
    "Kho dụng cụ",
)

PHONG_LIST = ["Phòng 1", "Phòng 2", "Phòng 4", "Hành chánh"]

CONG_VIEC_LIST = [
    "Vệ sinh phòng",
    "Vệ sinh hành lang + kiểm tra nước rửa tay",
    "Vệ sinh phòng nhận bệnh",
    "Thay nước ngâm dụng cụ",
    "Lãnh vật tư",
    "Gửi + quản lý dụng cụ + báo săng tồn",
    "Hỗ trợ hành chánh",
]

LIST_THANG = [f"{m:02d}/2026" for m in range(1, 13)]
KHO_EXPIRY_DAYS = int(os.getenv("KHO_EXPIRY_DAYS", "30"))


def _load_local_env_file() -> None:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    except Exception:
        # Giữ hành vi ổn định: nếu lỗi đọc .env thì bỏ qua và dùng env/secrets có sẵn.
        return


def _get_setting_with_source(*keys: str):
    normalized_keys = [k.strip() for k in keys if k and k.strip()]
    lower_keys = [k.lower() for k in normalized_keys]

    for key in keys:
        val = os.getenv(key, "")
        if val:
            return val.strip(), f"env:{key}"

    # 1) Top-level secrets: SUPABASE_URL, SUPABASE_KEY...
    for key in normalized_keys + lower_keys:
        try:
            val = st.secrets.get(key, "")
        except Exception:
            val = ""
        if isinstance(val, str) and val.strip():
            return val.strip(), f"secrets:{key}"

    # 2) Nested secrets:
    # [supabase]
    # url = "..."
    # key = "..."
    nested_candidates = ("supabase", "SUPABASE", "database", "DATABASE")
    for section_name in nested_candidates:
        try:
            section = st.secrets.get(section_name, {})
        except Exception:
            section = {}
        if not isinstance(section, dict):
            continue
        for key in normalized_keys + lower_keys:
            for candidate in (key, key.lower(), key.upper(), key.replace("SUPABASE_", "").lower()):
                val = section.get(candidate, "")
                if isinstance(val, str) and val.strip():
                    return val.strip(), f"secrets:{section_name}.{candidate}"
    return "", "missing"


def _get_setting(*keys: str) -> str:
    value, _ = _get_setting_with_source(*keys)
    return value


def _normalize_supabase_url(raw_url: str) -> str:
    url = (raw_url or "").strip()
    if not url:
        return ""
    if url.endswith("/"):
        url = url[:-1]
    if not url.endswith("/rest/v1"):
        if ".supabase.co" in url:
            url = f"{url}/rest/v1"
    return f"{url}/"


_load_local_env_file()
_RAW_SUPABASE_URL, _URL_SOURCE = _get_setting_with_source("SUPABASE_URL")
_RAW_SUPABASE_KEY, _KEY_SOURCE = _get_setting_with_source("SUPABASE_KEY", "API_KEY")

SUPABASE_URL = _normalize_supabase_url(_RAW_SUPABASE_URL)
SUPABASE_KEY = _RAW_SUPABASE_KEY

SUPABASE_DEBUG_INFO = {
    "url_source": _URL_SOURCE,
    "key_source": _KEY_SOURCE,
    "url_loaded": bool(SUPABASE_URL),
    "key_loaded": bool(SUPABASE_KEY),
    "key_length": len(SUPABASE_KEY),
}
