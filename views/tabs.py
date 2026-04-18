import calendar
import time
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from services.supabase import ghi_du_lieu_supabase, lay_du_lieu_supabase, xoa_dong_supabase
from utils.constants import CONG_VIEC_LIST, LIST_THANG, PHONG_LIST
from utils.data_helpers import get_fixed_order_list, stable_sort_dataframe


def render_tab_nhan_su_off(df_nhan_su_full: pd.DataFrame, danh_sach_ten: List[str]) -> None:
    st.header("📅 QUẢN LÝ NHÂN SỰ & ĐĂNG KÝ NGHỈ")
    with st.expander("👥 Danh sách nhân sự đang làm việc", expanded=False):
        if not df_nhan_su_full.empty:
            df_hien_thi = df_nhan_su_full[df_nhan_su_full["TRẠNG THÁI"].str.upper() == "ĐANG LÀM"]
            st.dataframe(df_hien_thi, use_container_width=True, hide_index=True)
    st.markdown("---")

    col_input, col_view = st.columns([1, 1])
    with col_input:
        st.subheader("📅 ĐĂNG KÝ NGHỈ (OFF/PHÉP)")
        if st.session_state["user_role"] == "ADMIN":
            nhan_vien_off = st.selectbox("Đăng ký nghỉ cho nhân viên:", danh_sach_ten)
        else:
            nhan_vien_off = st.selectbox("Đăng ký nghỉ cho:", [st.session_state["ho_ten"]], disabled=True)

        now = datetime.now()
        songay = calendar.monthrange(now.year, now.month)[1]
        list_ngay_trong_thang = [f"{i:02d}/{now.month:02d}/{now.year}" for i in range(1, songay + 1)]
        ngay_chon_roi_rac = st.multiselect(
            "Bấm để chọn các ngày nghỉ (ví dụ: 01, 03, 06...):",
            list_ngay_trong_thang,
        )
        if ngay_chon_roi_rac:
            st.write("📌 **Thiết lập loại nghỉ cho từng ngày:**")
            loai_nghi_dict: Dict[str, str] = {}
            cols = st.columns(3)
            for idx, ngay_str in enumerate(ngay_chon_roi_rac):
                with cols[idx % 3]:
                    loai_nghi_dict[ngay_str] = st.selectbox(
                        f"Ngày {ngay_str}:",
                        ["Off", "Phép", "1/2 Sáng", "1/2 Chiều"],
                        key=f"sel_{ngay_str}",
                    )
            ghi_chu_off = st.text_input("Ghi chú chung:")
            if st.button("🚀 XÁC NHẬN GỬI TẤT CẢ", use_container_width=True):
                with st.spinner("Đang lưu dữ liệu lên Supabase..."):
                    df_current_off = lay_du_lieu_supabase("dangkyoff_log")
                    thoi_diem_now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    du_lieu_gui: List[Dict[str, Any]] = []
                    ngay_bi_trung: List[str] = []
                    for ngay_str in ngay_chon_roi_rac:
                        if not df_current_off.empty:
                            is_exist = df_current_off[
                                (df_current_off["NGÀY NGHỈ"].astype(str) == ngay_str)
                                & (df_current_off["TÊN (ID)"].astype(str) == nhan_vien_off)
                            ]
                            if not is_exist.empty:
                                ngay_bi_trung.append(ngay_str)
                                continue
                        du_lieu_gui.append(
                            {
                                "NGÀY NGHỈ": ngay_str,
                                "TÊN (ID)": nhan_vien_off,
                                "LÝ DO": loai_nghi_dict[ngay_str],
                                "THỜI ĐIỂM ĐĂNG KÝ": thoi_diem_now,
                                "GHI CHÚ": ghi_chu_off,
                            }
                        )
                    if du_lieu_gui:
                        if ghi_du_lieu_supabase("dangkyoff_log", du_lieu_gui):
                            st.success(f"✅ Đã lưu thành công! (Log: {thoi_diem_now})")
                            if ngay_bi_trung:
                                st.warning(f"⚠️ Đã bỏ qua các ngày trùng: {', '.join(ngay_bi_trung)}")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Lỗi khi ghi vào Supabase!")
                    elif ngay_bi_trung:
                        st.error("Tất cả ngày đã chọn đều đã tồn tại!")

    with col_view:
        st.subheader("🔍 Theo dõi lịch nghỉ tháng này")
        df_off_raw = lay_du_lieu_supabase("dangkyoff_log")
        if df_off_raw.empty:
            st.info("Chưa có dữ liệu.")
        else:
            df_off_raw.columns = [str(c).strip().upper() for c in df_off_raw.columns]
            col_ly_do = [c for c in df_off_raw.columns if "LÝ DO" in c or "LY DO" in c]
            col_ngay = [c for c in df_off_raw.columns if "NGÀY" in c]
            col_ten = [c for c in df_off_raw.columns if "TÊN" in c]
            if not (col_ly_do and col_ngay and col_ten):
                st.warning("Không tìm thấy các cột dữ liệu cần thiết trên Supabase.")
            else:
                c_l, c_n, c_t = col_ly_do[0], col_ngay[0], col_ten[0]
                df_off_raw["DT"] = pd.to_datetime(df_off_raw[c_n], format="%d/%m/%Y", errors="coerce")
                thang_hien_tai = datetime.now().month
                df_month = df_off_raw[df_off_raw["DT"].dt.month == thang_hien_tai].copy()
                if df_month.empty:
                    st.info("Tháng này chưa có ai đăng ký.")
                else:
                    df_month = stable_sort_dataframe(df_month, primary_columns=["DT"], fallback_name_columns=[c_t, c_l])
                    summary = df_month.groupby(c_n, sort=False).agg({c_t: list, c_l: list}).reset_index()
                    for _, row in summary.iterrows():
                        with st.expander(f"📅 {row[c_n]} — 🔴 {len(row[c_t])} nhân sự nghỉ"):
                            for ten, loai in zip(row[c_t], row[c_l]):
                                st.write(f"• **{ten}**: {loai}")

    st.markdown("---")
    st.subheader("🗑️ Hủy đăng ký nghỉ")
    df_del_view = lay_du_lieu_supabase("dangkyoff_log")
    if df_del_view.empty:
        st.info("Không có dữ liệu để xóa.")
        return
    st.write("📌 Nhập mã ID từ bảng dưới đây để xóa:")
    st.dataframe(df_del_view.tail(5), use_container_width=True, hide_index=True)
    col_del1, col_del2 = st.columns([1, 1])
    with col_del1:
        id_to_delete = st.number_input("Nhập ID cần xóa:", min_value=1, step=1, key="id_del_input")
    with col_del2:
        st.write(" ")
        st.write(" ")
        if st.button("🔥 XÁC NHẬN XÓA", use_container_width=True):
            if xoa_dong_supabase("dangkyoff_log", int(id_to_delete)):
                st.success(f"✅ Đã xóa thành công dòng có ID: {id_to_delete}!")
                time.sleep(1)
                st.rerun()


def render_tab_phan_phong(danh_sach_ten: List[str]) -> None:
    st.header("📋 QUẢN LÝ PHÂN PHÒNG & CÔNG VIỆC")
    ngay_chon = st.date_input("Chọn ngày trực:", datetime.now())
    ngay_str = ngay_chon.strftime("%d/%m/%Y")

    list_nv_di_lam = danh_sach_ten
    df_off = lay_du_lieu_supabase("dangkyoff_log")
    if not df_off.empty:
        df_off.columns = [str(col).strip().upper() for col in df_off.columns]
        list_dang_off = (
            df_off[df_off["NGÀY NGHỈ"].astype(str).str.strip() == ngay_str]["TÊN (ID)"].astype(str).tolist()
        )
        list_nv_di_lam = [name for name in danh_sach_ten if name not in list_dang_off]
        if list_dang_off:
            st.warning(f"💡 Ngày {ngay_str} có {len(list_dang_off)} người nghỉ: {', '.join(list_dang_off)}")

    st.markdown("---")
    st.subheader(f"📅 BẢNG TRỰC ĐÃ PHÂN - {ngay_str}")
    df_view = lay_du_lieu_supabase("phanphong_2026")
    if df_view.empty:
        st.info("Chưa có dữ liệu lịch trực trên hệ thống.")
    else:
        df_view.columns = [str(c).strip().upper() for c in df_view.columns]
        res = df_view[df_view["NGÀY"].astype(str).str.strip() == ngay_str].copy()
        if res.empty:
            st.info(f"Ngày {ngay_str} chưa có lịch trực.")
        else:
            res = stable_sort_dataframe(res, primary_columns=["PHÒNG"], fallback_name_columns=["TÊN NHÂN VIÊN"])
            v_col1, v_col2 = st.columns(2)
            for idx, phong in enumerate(PHONG_LIST):
                col = v_col1 if idx < 2 else v_col2
                with col:
                    st.info(f"📍 **{phong.upper()}**")
                    p_data = res[res["PHÒNG"] == phong]
                    if p_data.empty:
                        st.write("🟢 Trống")
                    else:
                        for _, row in p_data.iterrows():
                            c_info, c_del = st.columns([4, 1])
                            with c_info:
                                st.write(f"👤 **{row['TÊN NHÂN VIÊN']}**")
                                st.caption(f"📝 {row['CÔNG VIỆC']}")
                            with c_del:
                                if st.session_state.get("user_role") == "ADMIN":
                                    key_xoa = f"del_{row['ID']}_{phong}_{idx}"
                                    if st.button("🗑️", key=key_xoa):
                                        if xoa_dong_supabase("phanphong_2026", int(row["ID"])):
                                            st.toast(f"Đã xóa {row['TÊN NHÂN VIÊN']}!")
                                            time.sleep(1)
                                            st.rerun()
                    st.markdown("---")

    if st.session_state["user_role"] != "ADMIN":
        return

    st.markdown("---")
    with st.form("form_tong_hop_supa"):
        st.subheader(f"✍️ Thiết lập lịch trực ngày {ngay_str}")
        main_col1, main_col2 = st.columns(2)
        du_lieu_nhap: Dict[str, Dict[str, Any]] = {}
        for idx, phong in enumerate(PHONG_LIST):
            target_col = main_col1 if idx % 2 == 0 else main_col2
            with target_col:
                st.info(f"📍 **{phong.upper()}**")
                nv1 = st.selectbox(f"NV 1 ({phong})", [""] + list_nv_di_lam, key=f"s_nv1_{phong}")
                v1 = st.multiselect(f"Việc NV 1", CONG_VIEC_LIST, key=f"s_v1_{phong}")
                nv2 = st.selectbox(f"NV 2 ({phong})", ["Không có"] + list_nv_di_lam, key=f"s_nv2_{phong}")
                v2 = st.multiselect(f"Việc NV 2", CONG_VIEC_LIST, key=f"s_v2_{phong}")
                st.markdown("---")
                du_lieu_nhap[phong] = {"nv1": nv1, "v1": v1, "nv2": nv2, "v2": v2}

        if st.form_submit_button("🚀 LƯU TOÀN BỘ LỊCH TRỰC", use_container_width=True):
            df_all = lay_du_lieu_supabase("phanphong_2026")
            final_rows: List[Dict[str, Any]] = []
            danh_sach_bi_trung: List[str] = []
            ten_da_co_lich: List[str] = []
            if not df_all.empty:
                df_all.columns = [str(c).upper() for c in df_all.columns]
                ten_da_co_lich = df_all[df_all["NGÀY"] == ngay_str]["TÊN NHÂN VIÊN"].astype(str).tolist()

            for phong, val in du_lieu_nhap.items():
                if val["nv1"] != "" and val["v1"]:
                    if val["nv1"] in ten_da_co_lich:
                        danh_sach_bi_trung.append(f"{val['nv1']} (Đã có lịch ngày {ngay_str})")
                    else:
                        final_rows.append(
                            {
                                "NGÀY": ngay_str,
                                "PHÒNG": phong,
                                "TÊN NHÂN VIÊN": val["nv1"],
                                "CÔNG VIỆC": ", ".join(val["v1"]),
                            }
                        )
                        ten_da_co_lich.append(val["nv1"])
                if val["nv2"] != "Không có" and val["v2"]:
                    if val["nv2"] in ten_da_co_lich:
                        danh_sach_bi_trung.append(f"{val['nv2']} (Đã có lịch ngày {ngay_str})")
                    else:
                        final_rows.append(
                            {
                                "NGÀY": ngay_str,
                                "PHÒNG": phong,
                                "TÊN NHÂN VIÊN": val["nv2"],
                                "CÔNG VIỆC": ", ".join(val["v2"]),
                            }
                        )
                        ten_da_co_lich.append(val["nv2"])

            if danh_sach_bi_trung:
                st.error(f"🚫 KHÔNG THỂ LƯU: {', '.join(set(danh_sach_bi_trung))}")
                st.warning("Vui lòng kiểm tra lại, một nhân sự không thể trực nhiều phòng trong một ngày.")
            elif final_rows:
                with st.spinner("Đang kiểm tra và lưu dữ liệu..."):
                    if ghi_du_lieu_supabase("phanphong_2026", final_rows):
                        st.success("✅ Đã cập nhật lịch trực thành công!")
                        time.sleep(1.5)
                        st.rerun()
                    else:
                        st.error("Lỗi khi gửi dữ liệu lên Supabase!")
            else:
                st.warning("Vui lòng chọn nhân sự trước khi bấm Lưu!")


def render_tab_tien_ca(df_nhan_su_full: pd.DataFrame, danh_sach_ten: List[str]) -> None:
    st.header("⚖️ ĐIỀU PHỐI THU NHẬP TỔ")
    thang_mac_dinh = datetime.now().strftime("%m/%Y")
    ngay_hom_nay = datetime.now().strftime("%d/%m/%Y")

    if st.session_state["user_role"] == "ADMIN":
        with st.expander("🛠️ KHU VỰC QUẢN LÝ (Nhập & Lưu dữ liệu)", expanded=True):
            ds_tat_ca = (
                df_nhan_su_full["TÊN (ID)"].tolist()
                if not df_nhan_su_full.empty and "TÊN (ID)" in df_nhan_su_full.columns
                else danh_sach_ten
            )
            ds_tat_ca = get_fixed_order_list(ds_tat_ca, danh_sach_ten)
            tab_input_ca, tab_input_hc = st.tabs(["💰 Nhập Tiền Ca", "🏢 Nhập Hành Chánh"])
            with tab_input_ca:
                col_ca1, col_ca2 = st.columns([1, 2])
                with col_ca1:
                    thang_ca_chon = st.selectbox(
                        "Tính cho tháng mấy:",
                        LIST_THANG,
                        index=LIST_THANG.index(thang_mac_dinh),
                        key="sel_thang_ca",
                    )
                with col_ca2:
                    nguoi_chia_ca = st.multiselect("Nhân sự tham gia:", options=ds_tat_ca, default=ds_tat_ca, key="ms_ca")
                if nguoi_chia_ca:
                    input_ca: Dict[str, int] = {}
                    c1, c2 = st.columns(2)
                    for i, name in enumerate(nguoi_chia_ca):
                        with c1 if i % 2 == 0 else c2:
                            input_ca[name] = st.number_input(
                                f"Tiền của {name}:",
                                min_value=0,
                                step=10000,
                                key=f"ad_ca_{name}",
                            )
                    if st.button("🚀 XÁC NHẬN & LƯU TIỀN CA"):
                        tong_ca = sum(input_ca.values())
                        bq_ca = tong_ca / len(nguoi_chia_ca)
                        data_to_save = []
                        for name, tien in input_ca.items():
                            chenh = bq_ca - tien
                            status = f"🟢 Nhận thêm {chenh:,.0f}" if chenh > 0 else f"🔴 Gửi lại {abs(chenh):,.0f}"
                            if chenh == 0:
                                status = "⚪ Đã đủ"
                            data_to_save.append(
                                {
                                    "NGÀY LẬP": ngay_hom_nay,
                                    "THÁNG": thang_ca_chon,
                                    "TÊN NHÂN VIÊN": name,
                                    "THỰC NHẬN": tien,
                                    "ĐIỀU PHỐI": status,
                                    "LOẠI TIỀN": "TIỀN CA",
                                }
                            )
                        if ghi_du_lieu_supabase("tienca_log", data_to_save):
                            st.success(f"✅ Đã lưu Tiền Ca tháng {thang_ca_chon}!")
                            time.sleep(1)
                            st.rerun()
            with tab_input_hc:
                col_hc1, col_hc2 = st.columns([1, 2])
                with col_hc1:
                    thang_hc_chon = st.selectbox(
                        "Tính cho tháng mấy:",
                        LIST_THANG,
                        index=LIST_THANG.index(thang_mac_dinh),
                        key="sel_thang_hc",
                    )
                with col_hc2:
                    nguoi_hanh_chanh = st.multiselect("Nhân sự nhận tiền HC:", options=ds_tat_ca, key="ms_hc")
                if nguoi_hanh_chanh:
                    input_hc: Dict[str, int] = {}
                    ch1, ch2 = st.columns(2)
                    for i, name in enumerate(nguoi_hanh_chanh):
                        with ch1 if i % 2 == 0 else ch2:
                            input_hc[name] = st.number_input(
                                f"Tiền HC {name}:",
                                min_value=0,
                                step=10000,
                                key=f"ad_hc_{name}",
                            )
                    if st.button("🚀 XÁC NHẬN & LƯU HÀNH CHÁNH"):
                        data_hc = []
                        for name, tien in input_hc.items():
                            data_hc.append(
                                {
                                    "NGÀY LẬP": ngay_hom_nay,
                                    "THÁNG": thang_hc_chon,
                                    "TÊN NHÂN VIÊN": name,
                                    "THỰC NHẬN": tien,
                                    "ĐIỀU PHỐI": "Chuyển Quỹ P.Thảo",
                                    "LOẠI TIỀN": "HÀNH CHÁNH",
                                }
                            )
                        if ghi_du_lieu_supabase("tienca_log", data_hc):
                            st.success(f"✅ Đã lưu tiền Hành Chánh tháng {thang_hc_chon}!")
                            time.sleep(1)
                            st.rerun()

    st.divider()
    st.subheader("📊 TRA CỨU BẢNG TỔNG HỢP")
    df_history = lay_du_lieu_supabase("tienca_log")
    if not df_history.empty:
        df_history.columns = [str(c).upper() for c in df_history.columns]
        df_history = stable_sort_dataframe(
            df_history,
            primary_columns=["THÁNG", "LOẠI TIỀN"],
            fallback_name_columns=["TÊN NHÂN VIÊN"],
        )

    v_ca, v_hc = st.tabs(["💰 Tiền Ca", "🏢 Hành Chánh"])
    with v_ca:
        thang_view_ca = st.selectbox("Xem dữ liệu tiền ca tháng:", LIST_THANG, key="view_thang_ca")
        if not df_history.empty:
            df_view_ca = df_history[(df_history["THÁNG"] == thang_view_ca) & (df_history["LOẠI TIỀN"] == "TIỀN CA")]
            if not df_view_ca.empty:
                tong = df_view_ca["THỰC NHẬN"].sum()
                st.info(f"Tổng quỹ ca tháng {thang_view_ca}: **{tong:,.0f}** | Bình quân: **{tong/len(df_view_ca):,.0f}**")
                st.table(df_view_ca[["TÊN NHÂN VIÊN", "THỰC NHẬN", "ĐIỀU PHỐI"]])
                st.success("📌 **Anh Tuấn làm đầu mối tập kết và chi trả phần Tiền Ca.**")
            else:
                st.write(f"Chưa có dữ liệu tiền ca cho tháng {thang_view_ca}.")
    with v_hc:
        thang_view_hc = st.selectbox("Xem dữ liệu hành chánh tháng:", LIST_THANG, key="view_thang_hc")
        if not df_history.empty:
            df_view_hc = df_history[(df_history["THÁNG"] == thang_view_hc) & (df_history["LOẠI TIỀN"] == "HÀNH CHÁNH")]
            if not df_view_hc.empty:
                st.info(f"Tổng tiền hành chánh tháng {thang_view_hc}: **{df_view_hc['THỰC NHẬN'].sum():,.0f} VNĐ**")
                st.table(df_view_hc[["TÊN NHÂN VIÊN", "THỰC NHẬN"]])
                st.warning("📌 **Tiền này chuyển về cho P.Thảo quản lý quỹ.**")
            else:
                st.write(f"Chưa có dữ liệu hành chánh cho tháng {thang_view_hc}.")


def _normalize_kho_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    result = df.copy()
    result.columns = [str(c).strip() for c in result.columns]
    rename_dict: Dict[str, str] = {}
    for col in result.columns:
        c_up = col.upper()
        if "TÊN BỘ" in c_up:
            rename_dict[col] = "TÊN BỘ DỤNG CỤ"
        if "TỒN" in c_up:
            rename_dict[col] = "TỒN SẴN SÀNG"
        if "ĐANG HẤP" in c_up:
            rename_dict[col] = "ĐANG HẤP"
        if "TÌNH TRẠNG" in c_up:
            rename_dict[col] = "TÌNH TRẠNG"
        if "SỐ LƯỢNG" in c_up:
            rename_dict[col] = "SỐ LƯỢNG"
    result = result.rename(columns=rename_dict)
    return result


def render_tab_kho_dung_cu(danh_sach_ten: List[str]) -> None:
    st.header("🏥 QUẢN LÝ DỤNG CỤ & TIỆT TRÙNG")
    df_dm = _normalize_kho_columns(lay_du_lieu_supabase("kho_danhmuc"))
    df_nk = _normalize_kho_columns(lay_du_lieu_supabase("kho_nhatky"))
    if df_dm.empty:
        st.warning("⚠️ Lỗi hệ thống: Không có dữ liệu kho_danhmuc.")
        return

    df_dm["TỒN SẴN SÀNG"] = pd.to_numeric(df_dm.get("TỒN SẴN SÀNG", 0), errors="coerce").fillna(0).astype(int)
    df_dm["ĐANG HẤP"] = pd.to_numeric(df_dm.get("ĐANG HẤP", 0), errors="coerce").fillna(0).astype(int)
    df_dm = stable_sort_dataframe(df_dm, fallback_name_columns=["TÊN BỘ DỤNG CỤ"])

    df_holding = df_nk[df_nk["TÌNH TRẠNG"] == "Đang giữ"] if not df_nk.empty else pd.DataFrame()

    def make_super_label(row: pd.Series) -> str:
        mon = row.get("TÊN BỘ DỤNG CỤ", "N/A")
        ton = row.get("TỒN SẴN SÀNG", 0)
        rel = df_holding[df_holding["TÊN BỘ DỤNG CỤ"] == mon] if not df_holding.empty else pd.DataFrame()
        h_qty = rel["SỐ LƯỢNG"].sum() if not rel.empty else 0
        who = ""
        if h_qty > 0:
            d_list = rel.groupby("NHÂN VIÊN")["SỐ LƯỢNG"].sum().reset_index()
            who = " | 🚩 Giữ: " + ", ".join([f"{r['NHÂN VIÊN']}:{r['SỐ LƯỢNG']}" for _, r in d_list.iterrows()])
        pfx = "⚠️ [HẾT] " if ton <= 0 else "✅ "
        return f"{pfx}{mon} (Tồn: {ton}{who})"

    df_dm["LABEL"] = df_dm.apply(make_super_label, axis=1)
    map_label_to_name = dict(zip(df_dm["LABEL"], df_dm["TÊN BỘ DỤNG CỤ"]))

    t1, t2, t3, t4 = st.tabs(["🚀 LẤY & CHỐT", "📤 GỬI TT", "📥 NHẬN VỀ", "📊 BÁO CÁO"])

    with t1:
        c1, c2 = st.columns([1.2, 1.8])
        with c1:
            st.subheader("📍 Lấy dụng cụ")
            with st.form("f_lay_v_final", clear_on_submit=True):
                nv_l = st.selectbox("Người lấy:", options=danh_sach_ten)
                ds_lbl = st.multiselect("Chọn dụng cụ:", options=df_dm["LABEL"].tolist())
                if st.form_submit_button("XÁC NHẬN LẤY") and ds_lbl:
                    for lbl in ds_lbl:
                        m_r = map_label_to_name[lbl]
                        ghi_du_lieu_supabase(
                            "kho_nhatky",
                            [
                                {
                                    "NGÀY GIỜ": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                                    "NHÂN VIÊN": nv_l,
                                    "HÀNH ĐỘNG": "LẤY",
                                    "TÊN BỘ DỤNG CỤ": m_r,
                                    "SỐ LƯỢNG": 1,
                                    "TÌNH TRẠNG": "Đang giữ",
                                }
                            ],
                        )
                        r_dm = df_dm[df_dm["TÊN BỘ DỤNG CỤ"] == m_r].iloc[0].to_dict()
                        d_id = r_dm.get("id", r_dm.get("ID"))
                        ghi_du_lieu_supabase(
                            "kho_danhmuc",
                            [{"id": int(d_id), "TỒN SẴN SÀNG": int(r_dm["TỒN SẴN SÀNG"]) - 1}],
                        )
                    st.rerun()
        with c2:
            st.subheader("📝 Chốt dùng")
            df_sua = df_nk[df_nk["TÌNH TRẠNG"].isin(["Đang giữ", "Chờ đi hấp"])] if not df_nk.empty else pd.DataFrame()
            if df_sua.empty:
                st.info("Trống.")
            else:
                for index, row in df_sua.iterrows():
                    r_id = row.get("id", row.get("ID", index))
                    with st.container(border=True):
                        ci, cq, cb = st.columns([2, 1, 1])
                        txt_tt = "🟢 Đang giữ" if row["TÌNH TRẠNG"] == "Đang giữ" else "🟡 Chờ hấp"
                        ci.write(f"{txt_tt} | **{row['NHÂN VIÊN']}**\n\n{row['TÊN BỘ DỤNG CỤ']}")
                        n_qty = cq.number_input("Số", 0, 100, int(row["SỐ LƯỢNG"]), key=f"ed_{index}")
                        if cb.button("CHỐT", key=f"btn_{index}"):
                            r_dm = df_dm[df_dm["TÊN BỘ DỤNG CỤ"] == row["TÊN BỘ DỤNG CỤ"]].iloc[0].to_dict()
                            dm_id = r_dm.get("id", r_dm.get("ID"))
                            diff = n_qty - int(row["SỐ LƯỢNG"])
                            ghi_du_lieu_supabase(
                                "kho_danhmuc",
                                [{"id": int(dm_id), "TỒN SẴN SÀNG": int(r_dm["TỒN SẴN SÀNG"]) - diff}],
                            )
                            up_data = {"id": int(r_id), "SỐ LƯỢNG": n_qty}
                            up_data["TÌNH TRẠNG"] = "Chờ đi hấp" if n_qty > 0 else "Đã xóa/Trả kho"
                            if ghi_du_lieu_supabase("kho_nhatky", [up_data]):
                                st.rerun()

    with t2:
        st.subheader("📤 Gửi đi hấp")
        c_date, _ = st.columns([1, 2])
        ngay_gui = c_date.date_input("Ngày gửi:", value=datetime.now())
        cho_g = df_nk[df_nk["TÌNH TRẠNG"] == "Chờ đi hấp"] if not df_nk.empty else pd.DataFrame()
        st.write("---")
        st.write("➕ **Gửi thêm đồ khác**")
        col_m, col_s, col_b_a = st.columns([2, 1, 1])
        m_them = col_m.selectbox("Bộ dụng cụ:", options=df_dm["TÊN BỘ DỤNG CỤ"].tolist(), key="add_g")
        s_them = col_s.number_input("SL:", 1, 100, 1, key="add_s")
        if col_b_a.button("THÊM"):
            ghi_du_lieu_supabase(
                "kho_nhatky",
                [
                    {
                        "NGÀY GIỜ": ngay_gui.strftime("%d/%m/%Y"),
                        "NHÂN VIÊN": "Hệ thống",
                        "HÀNH ĐỘNG": "GỬI THÊM",
                        "TÊN BỘ DỤNG CỤ": m_them,
                        "SỐ LƯỢNG": s_them,
                        "TÌNH TRẠNG": "Chờ đi hấp",
                    }
                ],
            )
            r_dm_t = df_dm[df_dm["TÊN BỘ DỤNG CỤ"] == m_them].iloc[0].to_dict()
            id_t = r_dm_t.get("id", r_dm_t.get("ID"))
            ghi_du_lieu_supabase(
                "kho_danhmuc",
                [{"id": int(id_t), "TỒN SẴN SÀNG": int(r_dm_t["TỒN SẴN SÀNG"]) - s_them}],
            )
            st.rerun()

        if not cho_g.empty:
            st.dataframe(cho_g[["NHÂN VIÊN", "TÊN BỘ DỤNG CỤ", "SỐ LƯỢNG"]], use_container_width=True)
            if st.button("🚀 XÁC NHẬN GỬI TOÀN BỘ"):
                ds_sum = cho_g.groupby("TÊN BỘ DỤNG CỤ")["SỐ LƯỢNG"].sum().reset_index()
                for _, row in ds_sum.iterrows():
                    r_dm_u = df_dm[df_dm["TÊN BỘ DỤNG CỤ"] == row["TÊN BỘ DỤNG CỤ"]].iloc[0].to_dict()
                    id_u = r_dm_u.get("id", r_dm_u.get("ID"))
                    ghi_du_lieu_supabase(
                        "kho_danhmuc",
                        [
                            {
                                "id": int(id_u),
                                "ĐANG HẤP": int(r_dm_u["ĐANG HẤP"]) + int(row["SỐ LƯỢNG"]),
                                "GHI CHÚ": ngay_gui.strftime("%d/%m/%Y"),
                            }
                        ],
                    )
                for _, r_nk in cho_g.iterrows():
                    ghi_du_lieu_supabase(
                        "kho_nhatky",
                        [
                            {
                                "id": int(r_nk.get("id", r_nk.get("ID"))),
                                "TÌNH TRẠNG": f"Đang hấp ({ngay_gui.strftime('%d/%m/%Y')})",
                            }
                        ],
                    )
                st.success("Đã gửi đi!")
                st.rerun()

    with t3:
        st.subheader("📥 Nhận về kho")
        m_v = st.selectbox("Bộ dụng cụ nhận về:", options=df_dm["TÊN BỘ DỤNG CỤ"].tolist(), key="n_v")
        s_v = st.number_input("Số lượng thực nhận:", 1, 100, 1, key="s_n")
        if st.button("XÁC NHẬN NHẬN"):
            r_dm_n = df_dm[df_dm["TÊN BỘ DỤNG CỤ"] == m_v].iloc[0].to_dict()
            id_n = r_dm_n.get("id", r_dm_n.get("ID"))
            ghi_du_lieu_supabase(
                "kho_danhmuc",
                [
                    {
                        "id": int(id_n),
                        "TỒN SẴN SÀNG": int(r_dm_n["TỒN SẴN SÀNG"]) + s_v,
                        "ĐANG HẤP": max(0, int(r_dm_n["ĐANG HẤP"]) - s_v),
                        "GHI CHÚ": datetime.now().strftime("%d/%m/%Y"),
                    }
                ],
            )
            st.rerun()

    with t4:
        st.subheader("📊 Báo cáo kho")
        st.dataframe(df_dm[[c for c in df_dm.columns if c != "LABEL"]], use_container_width=True)
