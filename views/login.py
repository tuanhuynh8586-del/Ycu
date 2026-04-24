import time
import uuid
import streamlit as st
from services.supabase import lay_du_lieu_supabase, update_remember_token_by_id

def login() -> None:
    st.title("🔑 ĐĂNG NHẬP HỆ THỐNG")

    with st.form("login_form"):
        # Dùng key tường minh để tránh Streamlit tự suy key theo label (dễ phát sinh lỗi lạ trên một số trình duyệt/IME).
        user_input = st.text_input("Tên đăng nhập (Username)", key="login_username").strip()
        pass_input_raw = st.text_input("Mật khẩu (Password)", type="password", key="login_password")
        pass_input = str(pass_input_raw or "").strip()

        # Workaround: một số trình duyệt/IME (đặc biệt mobile) có thể trả về chuỗi dạng escape như "\x31".
        # Nếu người dùng thực sự gõ ký tự '1' mà bị biến thành "\x31" thì chuyển ngược lại.
        if len(pass_input) == 4 and pass_input.startswith("\\x"):
            try:
                pass_input = chr(int(pass_input[2:], 16))
            except Exception:
                pass_input = str(pass_input_raw or "").strip()
        remember_me = st.checkbox("Duy trì đăng nhập")
        submit = st.form_submit_button("Đăng nhập")

        if not submit:
            return

        # Lấy dữ liệu user từ Supabase
        df_user = lay_du_lieu_supabase("nhansu_2026")
        if df_user.empty:
            st.error("Không tìm thấy dữ liệu nhân sự trên hệ thống!")
            return

        # ✅ chuẩn hoá username về lowercase để so sánh
        user_login = user_input.lower()
        match = df_user[
            (df_user["USERNAME"].astype(str).str.lower() == user_login)
            & (df_user["PASSWORD"].astype(str) == pass_input)
            & (df_user["TRẠNG THÁI"].astype(str).str.upper() != "NGHỈ LÀM")
        ]

        if match.empty:
            st.error("Tài khoản/Mật khẩu không đúng!")
            return

        # ✅ Lưu thông tin đăng nhập vào session
        st.session_state["logged_in"] = True
        st.session_state["ho_ten"] = match.iloc[0]["HỌ VÀ TÊN"]
        st.session_state["user_role"] = str(match.iloc[0]["ROLE"]).upper()

        def _set_rt_query_param(token: str) -> None:
            try:
                st.query_params["rt"] = token
            except Exception:
                pass

        def _clear_rt_query_param() -> None:
            try:
                st.query_params.pop("rt", None)
            except Exception:
                pass

        # ✅ Nếu chọn remember me thì tạo token và lưu vào Supabase
        if remember_me:
            token = str(uuid.uuid4()).lower()
            # update theo id để chắc chắn match 1 dòng (tránh lệch hoa/thường USERNAME)
            user_id = match.iloc[0].get("id", match.iloc[0].get("ID"))
            ok = update_remember_token_by_id(user_id, token)
            if ok:
                # dùng chung key với app.py để auto-login + persist qua F5 bằng query param
                st.session_state["remember_token"] = token
                _set_rt_query_param(token)
            else:
                st.warning("Không thể ghi token duy trì đăng nhập lên Supabase. Vui lòng thử lại.")
        else:
            st.session_state.pop("remember_token", None)
            _clear_rt_query_param()

        st.success(f"Chào {st.session_state['ho_ten']}!")
        time.sleep(1)
        st.rerun()
