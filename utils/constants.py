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


def _get_setting(*keys: str) -> str:
    for key in keys:
        val = os.getenv(key, "")
        if val:
            return val.strip()
    for key in keys:
        try:
            val = st.secrets.get(key, "")
        except Exception:
            val = ""
        if val:
            return str(val).strip()
    return ""


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
SUPABASE_URL = _normalize_supabase_url(_get_setting("SUPABASE_URL"))
SUPABASE_KEY = _get_setting("SUPABASE_KEY", "API_KEY")
