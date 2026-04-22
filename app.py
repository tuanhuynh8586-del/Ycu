import streamlit as st
from services.data_loader import get_danh_sach_ten, load_nhan_su_data
from services.supabase import update_user_password
from utils.constants import MENU_ITEMS
from utils.state import init_session_state
from views.login import login
from views.tabs import (
    render_tab_kho_dung_cu,
    render_tab_nhan_su_off,
    render_tab_phan_phong,
    render_tab_tien_ca,
)

st.set_page_config(page_title="QUẢN LÝ Y CỤ LẦU 2", layout="wide")
init_session_state()

if not st.session_state["logged_in"]:
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
    if st.button("🔒 Đổi mật khẩu", use_container_width=True):
        st.session_state["show_change_password"] = not st.session_state.get("show_change_password", False)
    if st.session_state.get("show_change_password", False):
        with st.form("change_password_form", clear_on_submit=True):
            current_password = st.text_input("Mật khẩu hiện tại", type="password")
            new_password = st.text_input("Mật khẩu mới", type="password")
            confirm_password = st.text_input("Xác nhận mật khẩu mới", type="password")
            submitted_change = st.form_submit_button("Cập nhật mật khẩu", use_container_width=True)
            if submitted_change:
                if not current_password or not new_password or not confirm_password:
                    st.error("Vui lòng nhập đầy đủ thông tin.")
                elif new_password != confirm_password:
                    st.error("Mật khẩu mới và xác nhận chưa khớp.")
                elif len(new_password) < 6:
                    st.error("Mật khẩu mới phải có ít nhất 6 ký tự.")
                else:
                    ok, msg = update_user_password(st.session_state.get("username", ""), current_password, new_password)
                    if ok:
                        st.success(msg)
                        st.session_state["show_change_password"] = False
                    else:
                        st.error(msg)
    if st.button("Đăng xuất", use_container_width=True):
        remembered_username = st.session_state.get("remembered_username", "")
        remember_me = st.session_state.get("remember_me", False)
        st.session_state["logged_in"] = False
        st.session_state["ho_ten"] = ""
        st.session_state["user_role"] = ""
        st.session_state["username"] = ""
        st.session_state["auth_user_id"] = None
        st.session_state["show_change_password"] = False
        st.session_state["remembered_username"] = remembered_username if remember_me else ""
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