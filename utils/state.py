import streamlit as st


def init_session_state() -> None:
    defaults = {
        "logged_in": False,
        "ho_ten": "",
        "user_role": "",
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val
