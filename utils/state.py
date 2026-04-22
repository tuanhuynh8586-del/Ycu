import streamlit as st


def init_session_state() -> None:
    defaults = {
        "logged_in": False,
        "ho_ten": "",
        "user_role": "",
        "username": "",
        "auth_user_id": None,
        "remember_me": False,
        "remembered_username": "",
        "show_change_password": False,
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val
