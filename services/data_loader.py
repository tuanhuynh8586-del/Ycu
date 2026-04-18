import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

from services.supabase import lay_du_lieu_supabase
from utils.data_helpers import get_fixed_order_list, stable_sort_dataframe


@st.cache_resource
def get_google_sheet_client() -> gspread.Client:
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file("key.json", scopes=scope)
    return gspread.authorize(creds)


def load_nhan_su_data() -> pd.DataFrame:
    df_nhan_su = lay_du_lieu_supabase("nhansu_2026")
    if not df_nhan_su.empty:
        return stable_sort_dataframe(
            df_nhan_su,
            fallback_name_columns=["TÊN (ID)", "HỌ VÀ TÊN"],
        )

    client = get_google_sheet_client()
    sh = client.open("Quan_Ly_To_Y_Cu_2026")
    ws_ns = sh.worksheet("Nhansu_2026")
    df_fallback = pd.DataFrame(ws_ns.get_all_records())
    df_fallback.columns = [str(c).strip().upper() for c in df_fallback.columns]
    return stable_sort_dataframe(
        df_fallback,
        fallback_name_columns=["TÊN (ID)", "HỌ VÀ TÊN"],
    )


def get_danh_sach_ten(df_nhan_su_full: pd.DataFrame):
    if df_nhan_su_full.empty:
        return []
    df_active = df_nhan_su_full[df_nhan_su_full["TRẠNG THÁI"].astype(str).str.upper() == "ĐANG LÀM"].copy()
    ordered_names = get_fixed_order_list(df_active["TÊN (ID)"].tolist())
    return ordered_names
