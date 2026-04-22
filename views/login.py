import time

import streamlit as st

from services.supabase import get_user_by_username


def login() -> None:
    st.title("🔑 ĐĂNG NHẬP HỆ THỐNG")
    with st.form("login_form"):
        default_username = st.session_state.get("remembered_username", "")
        user_input = st.text_input("Tên đăng nhập (Username)", value=default_username).strip()
        pass_input = st.text_input("Mật khẩu (Password)", type="password").strip()
        remember_me = st.checkbox("Remember me", value=bool(default_username))
        submit = st.form_submit_button("Đăng nhập")
        if not submit:
            return

        user_row, err = get_user_by_username(user_input)
        if user_row is None:
            st.error(err)
            return

        if str(user_row.get("PASSWORD", "")) != pass_input:
            st.error("Tài khoản/Mật khẩu không đúng!")
            return

        st.session_state["logged_in"] = True
        st.session_state["username"] = str(user_row.get("USERNAME", "")).strip()
        st.session_state["auth_user_id"] = user_row.get("id", user_row.get("ID"))
        st.session_state["ho_ten"] = user_row.get("HỌ VÀ TÊN", "")
        st.session_state["user_role"] = str(user_row.get("ROLE", "")).upper()
        st.session_state["remember_me"] = bool(remember_me)
        st.session_state["remembered_username"] = st.session_state["username"] if remember_me else ""
        st.success(f"Chào {st.session_state['ho_ten']}!")
        time.sleep(1)
        st.rerun()
