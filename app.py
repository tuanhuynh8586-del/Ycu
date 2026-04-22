import streamlit as st
from services.data_loader import get_danh_sach_ten, load_nhan_su_data
from utils.constants import MENU_ITEMS
from utils.state import init_session_state
from views.login import login
from views.tabs import (
    render_tab_kho_dung_cu,
    render_tab_nhan_su_off,
    render_tab_phan_phong,
    render_tab_tien_ca,
)
from services.supabase import get_user_by_token   # ✅ thêm import

st.set_page_config(page_title="QUẢN LÝ Y CỤ LẦU 2", layout="wide")
init_session_state()

def _get_remember_token_from_session() -> str:
    # Backward-compat: chấp nhận cả key cũ viết hoa
    token = st.session_state.get("remember_token") or st.session_state.get("REMEMBER_TOKEN")
    return str(token or "").strip()

# Ưu tiên token từ session, fallback token từ URL query param (persist qua F5)
def _get_remember_token_from_url() -> str:
    try:
        raw = st.query_params.get("rt", "")
    except Exception:
        raw = ""
    if isinstance(raw, list):
        raw = raw[0] if raw else ""
    return str(raw or "").strip()

# ✅ kiểm tra token remember me trước
remember_token = _get_remember_token_from_session() or _get_remember_token_from_url()
if remember_token and not st.session_state.get("logged_in", False):
    user = get_user_by_token(remember_token)
    if user:
        st.session_state["logged_in"] = True
        st.session_state["ho_ten"] = user["HỌ VÀ TÊN"]
        st.session_state["user_role"] = str(user["ROLE"]).upper()
        st.session_state["remember_token"] = remember_token
        try:
            st.query_params["rt"] = remember_token
        except Exception:
            pass

# ✅ nếu chưa đăng nhập thì gọi login
if not st.session_state.get("logged_in", False):
    login()
    st.stop()

try:
    df_nhan_su_full = load_nhan_su_data()
    danh_sach_ten = get_danh_sach_ten(df_nhan_su_full)
except Exception as exc:
    st.error(f"Lỗi khởi tạo dữ liệu: {exc}")
    st.stop()

with st.sidebar:
    st.markdown(f"### 👤 Chào {st.session_state['ho_ten']}")
    st.info(f"🔑 Quyền: **{st.session_state['user_role']}**")
    if st.button("Đăng xuất", use_container_width=True):
        st.session_state["logged_in"] = False
        st.session_state.pop("remember_token", None)
        st.session_state.pop("REMEMBER_TOKEN", None)
        try:
            st.query_params.pop("rt", None)
        except Exception:
            pass
        st.rerun()

st.sidebar.title("🏥 QUẢN LÝ TỔ Y CỤ")
menu = st.sidebar.radio("Chọn chức năng:", MENU_ITEMS)

if menu == "Nhân sự & Đăng ký Off":
    render_tab_nhan_su_off(df_nhan_su_full, danh_sach_ten)
elif menu == "Phân phòng trực":
    render_tab_phan_phong(danh_sach_ten)
elif menu == "Tiền ca & Điều phối":
    render_tab_tien_ca(df_nhan_su_full, danh_sach_ten)
elif menu == "Kho dụng cụ":
    try:
        render_tab_kho_dung_cu(danh_sach_ten)
    except Exception as exc:
        st.error(f"⚠️ Lỗi hệ thống: {exc}")
