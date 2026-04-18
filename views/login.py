import time

import streamlit as st

from services.supabase import lay_du_lieu_supabase


def login() -> None:
    st.title("🔑 ĐĂNG NHẬP HỆ THỐNG")
    with st.form("login_form"):
        user_input = st.text_input("Tên đăng nhập (Username)").strip()
        pass_input = st.text_input("Mật khẩu (Password)", type="password").strip()
        submit = st.form_submit_button("Đăng nhập")
        if not submit:
            return

        df_user = lay_du_lieu_supabase("nhansu_2026")
        if df_user.empty:
            st.error("Không tìm thấy dữ liệu nhân sự trên hệ thống!")
            return

        user_login = user_input.lower()
        match = df_user[
            (df_user["USERNAME"].astype(str).str.lower() == user_login)
            & (df_user["PASSWORD"].astype(str) == pass_input)
            & (df_user["TRẠNG THÁI"].astype(str).str.upper() != "NGHỈ LÀM")
        ]
        if match.empty:
            st.error("Tài khoản/Mật khẩu không đúng!")
            return

        st.session_state["logged_in"] = True
        st.session_state["ho_ten"] = match.iloc[0]["HỌ VÀ TÊN"]
        st.session_state["user_role"] = str(match.iloc[0]["ROLE"]).upper()
        st.success(f"Chào {st.session_state['ho_ten']}!")
        time.sleep(1)
        st.rerun()
