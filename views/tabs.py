import calendar
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import streamlit as st
import pandas as pd

from services.supabase import (
    deduct_batch,
    ghi_du_lieu_supabase,
    insert_batch,
    lay_du_lieu_supabase,
    log_usage,
    log_tools_received_with_expiry,
    log_tools_sent_for_sterilization,
    xoa_dong_supabase,
)
from utils.constants import CONG_VIEC_LIST, KHO_EXPIRY_DAYS, LIST_THANG, PHONG_LIST
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
        # Mobile-friendly: chọn 1 ngày trong tháng/năm cần đăng ký (từ đó suy ra tháng/năm)
        month_anchor = st.date_input(
            "Chọn tháng/năm cần đăng ký (chọn đại 1 ngày trong tháng):",
            value=now.date(),
            key="off_month_anchor",
        )
        month_selected = int(month_anchor.month)
        year_selected = int(month_anchor.year)

        songay = calendar.monthrange(year_selected, month_selected)[1]
        list_ngay_trong_thang = [f"{i:02d}/{month_selected:02d}/{year_selected}" for i in range(1, songay + 1)]
        ngay_chon_roi_rac = st.multiselect(
            "Bấm để chọn các ngày nghỉ (ví dụ: 01, 03, 06...):",
            list_ngay_trong_thang,
            key=f"ms_off_days_{month_selected:02d}_{year_selected}",
        )
        if ngay_chon_roi_rac:
            st.write("📌 **Thiết lập loại nghỉ cho từng ngày:**")
            loai_nghi_dict: Dict[str, str] = {}
            # Mobile-friendly: 2 cột thay vì 3 để khỏi bị chật màn hình
            cols = st.columns(2)
            for idx, ngay_str in enumerate(ngay_chon_roi_rac):
                with cols[idx % 2]:
                    loai_nghi_dict[ngay_str] = st.selectbox(
                        f"Ngày {ngay_str}:",
                        ["Off", "Phép", "1/2 Sáng", "1/2 Chiều"],
                        key=f"sel_{ngay_str}_{nhan_vien_off}",
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
        st.subheader("🔍 Theo dõi lịch nghỉ theo tháng")
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
                # Bám theo tháng/năm đang chọn ở khung đăng ký để xem đúng dữ liệu
                df_month = df_off_raw[
                    (df_off_raw["DT"].dt.month == int(month_selected)) & (df_off_raw["DT"].dt.year == int(year_selected))
                ].copy()
                if df_month.empty:
                    st.info("Tháng đang chọn chưa có ai đăng ký.")
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
    df_del_view.columns = [str(c).strip().upper() for c in df_del_view.columns]
    if "NGÀY NGHỈ" in df_del_view.columns:
        df_del_view["DT"] = pd.to_datetime(df_del_view["NGÀY NGHỈ"], format="%d/%m/%Y", errors="coerce")
        df_del_view = df_del_view[
            (df_del_view["DT"].dt.month == int(month_selected)) & (df_del_view["DT"].dt.year == int(year_selected))
        ].copy()
        df_del_view = stable_sort_dataframe(df_del_view, primary_columns=["DT", "id"], fallback_name_columns=["TÊN (ID)"])
    st.dataframe(df_del_view.tail(10), use_container_width=True, hide_index=True)
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
        c_up = str(col).strip().upper()
        if c_up in ("TEN_DUNG_CU", "TOOL_NAME", "TÊN DỤNG CỤ", "TÊN BỘ DỤNG CỤ") or "TÊN BỘ" in c_up:
            rename_dict[col] = "TÊN BỘ DỤNG CỤ"
        elif "TỒN" in c_up or c_up in ("TON_SAN_SANG", "TON_SAN", "TON"):
            rename_dict[col] = "TỒN SẴN SÀNG"
        elif "ĐANG HẤP" in c_up or c_up == "DANG HAP":
            rename_dict[col] = "ĐANG HẤP"
        elif c_up in ("TÌNH TRẠNG", "TRANG THAI", "STATUS"):
            rename_dict[col] = "TÌNH TRẠNG"
        elif c_up in ("SỐ LƯỢNG", "SO LUONG", "SO_LUONG", "QUANTITY"):
            rename_dict[col] = "SỐ LƯỢNG"
    result = result.rename(columns=rename_dict)
    return _safe_merge_duplicate_columns(result)


def _ensure_tool_name_column(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "TÊN BỘ DỤNG CỤ" not in df.columns:
        for alt in ("TEN_DUNG_CU", "TOOL_NAME", "TÊN DỤNG CỤ"):
            if alt in df.columns:
                df["TÊN BỘ DỤNG CỤ"] = df[alt].astype(str)
                break
    return df


def _safe_merge_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    result = df.copy()
    columns = list(result.columns)
    duplicates = [col for idx, col in enumerate(columns) if col in columns[:idx]]
    for dup in set(duplicates):
        dup_locs = [idx for idx, col in enumerate(columns) if col == dup]
        if len(dup_locs) <= 1:
            continue
        work = result.iloc[:, dup_locs]
        if all(pd.api.types.is_numeric_dtype(work.iloc[:, idx].dtype) for idx in range(work.shape[1])):
            result[dup] = work.apply(pd.to_numeric, errors="coerce").sum(axis=1, skipna=True).fillna(0)
        else:
            def first_nonempty(values: pd.Series) -> Any:
                for value in values:
                    if pd.notna(value) and str(value).strip() != "":
                        return value
                return ""
            result[dup] = work.apply(first_nonempty, axis=1)
        drop_cols = [columns[i] for i in dup_locs[1:]]
        result = result.drop(columns=drop_cols, errors="ignore")
        columns = list(result.columns)
    return result.loc[:, ~result.columns.duplicated()]


def _group_duplicate_tool_rows(df: pd.DataFrame, tool_col: str = "TÊN BỘ DỤNG CỤ") -> pd.DataFrame:
    if df.empty or tool_col not in df.columns:
        return df
    agg_map: Dict[str, Any] = {}
    for col in df.columns:
        if col == tool_col:
            agg_map[col] = "first"
        elif pd.api.types.is_numeric_dtype(df[col].dtype):
            agg_map[col] = "sum"
        else:
            agg_map[col] = "first"
    return df.groupby(tool_col, as_index=False).agg(agg_map)


def _group_tool_log_view(
    df: pd.DataFrame,
    tool_cols: List[str],
    qty_cols: List[str],
    date_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    if df.empty:
        return df
    tool_col = next((c for c in tool_cols if c in df.columns), None)
    qty_col = next((c for c in qty_cols if c in df.columns), None)
    date_col = next((c for c in (date_cols or []) if c in df.columns), None)
    if tool_col is None or qty_col is None:
        return df
    agg_map: Dict[str, Any] = {tool_col: "first", qty_col: "sum"}
    if date_col:
        agg_map[date_col] = "first"
    grouped = df.groupby(tool_col, as_index=False).agg(agg_map)
    if date_col:
        return grouped[[tool_col, qty_col, date_col]]
    return grouped[[tool_col, qty_col]]


def _parse_datetime_safe(value: Any) -> pd.Timestamp:
    if value is None or str(value).strip() == "":
        return pd.NaT
    val = str(value).strip()
    parsed = pd.to_datetime(val, format="%d/%m/%Y %H:%M:%S", errors="coerce")
    if pd.isna(parsed):
        parsed = pd.to_datetime(val, format="%d/%m/%Y", errors="coerce")
    if pd.isna(parsed):
        parsed = pd.to_datetime(val, errors="coerce")
    return parsed


def _parse_date_safe(value: Any) -> Optional[date]:
    dt = _parse_datetime_safe(value)
    if pd.isna(dt):
        return None
    return dt.date()


def _normalize_fifo_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out.columns = [str(c).strip().upper() if str(c).strip().lower() != "id" else "id" for c in out.columns]
    rename_map: Dict[str, str] = {}
    for col in out.columns:
        c_up = str(col).upper()
        if "TOOL_NAME" == c_up or "TÊN BỘ DỤNG CỤ" in c_up:
            rename_map[col] = "TOOL_NAME"
        elif c_up in ("QUANTITY", "SỐ LƯỢNG", "SO LUONG"):
            rename_map[col] = "QUANTITY"
        elif c_up in ("REMAINING_QTY", "SL_CÒN", "SL CON", "SO LUONG CON", "SO_LUONG_CON"):
            rename_map[col] = "REMAINING_QTY"
        elif c_up in ("DATE_RECEIVED", "NGÀY NHẬN", "NGAY NHAN"):
            rename_map[col] = "DATE_RECEIVED"
        elif c_up in ("DATE_RECEIVED_DATE", "NGAY_NHAN_DATE"):
            rename_map[col] = "DATE_RECEIVED_DATE"
        elif c_up in ("EXPIRY_DATE", "HẠN DÙNG", "HAN DUNG"):
            rename_map[col] = "EXPIRY_DATE"
        elif c_up in ("EXPIRY_DATE_DATE", "HAN_DUNG_DATE"):
            rename_map[col] = "EXPIRY_DATE_DATE"
    return out.rename(columns=rename_map)


def _build_send_log_from_nhatky(df_nk: pd.DataFrame) -> pd.DataFrame:
    if df_nk.empty:
        return pd.DataFrame()
    src = df_nk.copy()
    if "TÌNH TRẠNG" not in src.columns:
        return pd.DataFrame()
    mask = src["TÌNH TRẠNG"].astype(str).str.contains("Đang hấp", case=False, na=False)
    src = src[mask].copy()
    if src.empty:
        return src
    src["TIMESTAMP_SENT"] = src.get("NGÀY GIỜ", "").astype(str)
    src["TOOL_NAME"] = src.get("TÊN BỘ DỤNG CỤ", "").astype(str)
    src["QUANTITY_SENT"] = pd.to_numeric(src.get("SỐ LƯỢNG", 0), errors="coerce").fillna(0).astype(int)
    src["__ts"] = src["TIMESTAMP_SENT"].apply(_parse_datetime_safe)
    return stable_sort_dataframe(
        src,
        primary_columns=["__ts", "id"],
        fallback_name_columns=["TOOL_NAME", "NHÂN VIÊN"],
    )


def _build_receive_log_from_fifo(df_fifo_raw: pd.DataFrame) -> pd.DataFrame:
    df_fifo = _normalize_fifo_columns(df_fifo_raw)
    if df_fifo.empty:
        return pd.DataFrame()
    required = {"TOOL_NAME", "QUANTITY", "DATE_RECEIVED", "EXPIRY_DATE"}
    if not required.issubset(df_fifo.columns):
        return pd.DataFrame()

    # Chuẩn hóa số lượng
    df_fifo["QUANTITY"] = pd.to_numeric(df_fifo["QUANTITY"], errors="coerce").fillna(0).astype(int)
    if "REMAINING_QTY" in df_fifo.columns:
        df_fifo["REMAINING_QTY"] = pd.to_numeric(df_fifo["REMAINING_QTY"], errors="coerce").fillna(0).astype(int)
    else:
        df_fifo["REMAINING_QTY"] = df_fifo["QUANTITY"]

    # Chuẩn hóa ngày nhận và hạn dùng
    if "DATE_RECEIVED_DATE" in df_fifo.columns:
        df_fifo["__rcv"] = pd.to_datetime(df_fifo["DATE_RECEIVED_DATE"], errors="coerce")
    else:
        df_fifo["__rcv"] = df_fifo["DATE_RECEIVED"].apply(_parse_datetime_safe)
    if "EXPIRY_DATE_DATE" in df_fifo.columns:
        df_fifo["__exp"] = pd.to_datetime(df_fifo["EXPIRY_DATE_DATE"], errors="coerce")
    else:
        df_fifo["__exp"] = df_fifo["EXPIRY_DATE"].apply(_parse_datetime_safe)

    # 👉 Gộp theo TOOL_NAME, bỏ id để tránh tách dòng
    df_fifo_grouped = (
        df_fifo.groupby("TOOL_NAME", as_index=False)
        .agg({
            "QUANTITY": "sum",
            "REMAINING_QTY": "sum",
            "__rcv": "min",   # ngày nhận sớm nhất
            "__exp": "max"    # hạn dùng muộn nhất
        })
    )

    # Sort ổn định
    return stable_sort_dataframe(
        df_fifo_grouped,
        primary_columns=["__rcv", "__exp"],
        fallback_name_columns=["TOOL_NAME"],
    )




def _consume_fifo_lots(df_fifo: pd.DataFrame, tool_name: str, qty_to_consume: int) -> List[Dict[str, Any]]:
    if df_fifo.empty or qty_to_consume <= 0:
        return []
    if "TOOL_NAME" not in df_fifo.columns or "REMAINING_QTY" not in df_fifo.columns:
        return []
    work = df_fifo.copy()
    work["REMAINING_QTY"] = pd.to_numeric(work["REMAINING_QTY"], errors="coerce").fillna(0).astype(int)
    work["__exp"] = work["EXPIRY_DATE"].apply(_parse_datetime_safe) if "EXPIRY_DATE" in work.columns else pd.NaT
    work = stable_sort_dataframe(work, primary_columns=["__exp", "DATE_RECEIVED", "id"], fallback_name_columns=["TOOL_NAME"])
    remain = int(qty_to_consume)
    updates: List[Dict[str, Any]] = []
    for _, row in work.iterrows():
        if str(row.get("TOOL_NAME", "")) != str(tool_name):
            continue
        lot_id = row.get("id", row.get("ID"))
        if lot_id is None:
            continue
        lot_remaining = int(row.get("REMAINING_QTY", 0))
        if lot_remaining <= 0:
            continue
        take = min(lot_remaining, remain)
        new_remaining = lot_remaining - take
        updates.append({"id": int(float(lot_id)), "REMAINING_QTY": int(new_remaining)})
        remain -= take
        if remain <= 0:
            break
    return updates


def _fefo_priority_label(sorted_idx: int) -> str:
    if sorted_idx == 0:
        return "HIGH"
    if sorted_idx <= 2:
        return "MEDIUM"
    return "LOW"


def _fefo_priority_badge(priority: str) -> str:
    p = str(priority).upper()
    if p == "HIGH":
        return "🔴 HIGH"
    if p == "MEDIUM":
        return "🟡 MEDIUM"
    return "🟢 LOW"


def _normalize_batch_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out.columns = [str(c).strip().upper() if str(c).strip().lower() != "id" else "id" for c in out.columns]
    rename_map: Dict[str, str] = {}
    for col in out.columns:
        c = str(col).upper()
        if c in ("TEN_DUNG_CU", "TÊN DỤNG CỤ", "TOOL_NAME", "TÊN BỘ DỤNG CỤ"):
            rename_map[col] = "TEN_DUNG_CU"
        elif c in ("NGAY_HAP", "NGÀY HẤP", "DATE_RECEIVED"):
            rename_map[col] = "NGAY_HAP"
        elif c in ("NGAY_HAP_DATE", "DATE_RECEIVED_DATE"):
            rename_map[col] = "NGAY_HAP_DATE"
        elif c in ("SO_LUONG", "SỐ LƯỢNG", "QUANTITY", "REMAINING_QTY"):
            rename_map[col] = "SO_LUONG"
        elif c in ("HAN_DUNG", "HẠN DÙNG", "EXPIRY_DATE"):
            rename_map[col] = "HAN_DUNG"
        elif c in ("HAN_DUNG_DATE", "EXPIRY_DATE_DATE"):
            rename_map[col] = "HAN_DUNG_DATE"
        elif c in ("TRANG_THAI", "TRẠNG THÁI", "STATUS"):
            rename_map[col] = "TRANG_THAI"
    renamed = out.rename(columns=rename_map)
    return _safe_merge_duplicate_columns(renamed)


def _build_fefo_tool_priority(df_batches: pd.DataFrame) -> pd.DataFrame:
    if df_batches.empty:
        return pd.DataFrame()
    work = _normalize_batch_columns(df_batches).copy()
    if not {"TEN_DUNG_CU", "SO_LUONG"}.issubset(work.columns):
        return pd.DataFrame()
    work["SO_LUONG"] = pd.to_numeric(work["SO_LUONG"], errors="coerce").fillna(0).astype(int)
    work = work[work["SO_LUONG"] > 0].copy()
    if work.empty:
        return pd.DataFrame()
    if "TRANG_THAI" in work.columns:
        work = work[work["TRANG_THAI"].astype(str).str.lower() == "ready"].copy()
    if work.empty:
        return pd.DataFrame()
    if "HAN_DUNG_DATE" in work.columns:
        work["__exp"] = pd.to_datetime(work["HAN_DUNG_DATE"], errors="coerce")
    else:
        work["__exp"] = work.get("HAN_DUNG", pd.Series(dtype="object")).apply(_parse_datetime_safe)
    if "NGAY_HAP_DATE" in work.columns:
        work["__nhap"] = pd.to_datetime(work["NGAY_HAP_DATE"], errors="coerce")
    else:
        work["__nhap"] = work.get("NGAY_HAP", pd.Series(dtype="object")).apply(_parse_datetime_safe)
    work = stable_sort_dataframe(
        work,
        primary_columns=["__exp", "__nhap", "id"],
        fallback_name_columns=["TEN_DUNG_CU"],
    )
    summary = (
        work.groupby("TEN_DUNG_CU", as_index=False)
        .agg(
            SO_LO=("TEN_DUNG_CU", "count"),
            TONG_SO_LUONG=("SO_LUONG", "sum"),
            HAN_GAN_NHAT=("__exp", "min"),
        )
        .sort_values(by=["HAN_GAN_NHAT", "TEN_DUNG_CU"], kind="stable", na_position="last")
    )
    today = pd.Timestamp(datetime.now().date())
    summary["SO_NGAY_CON_LAI"] = (summary["HAN_GAN_NHAT"] - today).dt.days
    return summary


def _get_fefo_batches_from_cache(df_batches: pd.DataFrame, tool_name: str) -> pd.DataFrame:
    if df_batches.empty or not str(tool_name).strip():
        return pd.DataFrame()
    work = _normalize_batch_columns(df_batches).copy()
    if not {"TEN_DUNG_CU", "SO_LUONG"}.issubset(work.columns):
        return pd.DataFrame()
    if "TRANG_THAI" in work.columns:
        work = work[work["TRANG_THAI"].astype(str).str.strip().str.lower() == "ready"]
    tool_name_key = str(tool_name).strip().lower()
    work = work[
        work["TEN_DUNG_CU"].astype(str).str.strip().str.lower() == tool_name_key
    ].copy()
    work["SO_LUONG"] = pd.to_numeric(work["SO_LUONG"], errors="coerce").fillna(0).astype(int)
    work = work[work["SO_LUONG"] > 0].copy()
    if "HAN_DUNG_DATE" in work.columns:
        work["__exp"] = pd.to_datetime(work["HAN_DUNG_DATE"], errors="coerce")
    else:
        work["__exp"] = work.get("HAN_DUNG", pd.Series(dtype="object")).apply(_parse_datetime_safe)
    if "NGAY_HAP_DATE" in work.columns:
        work["__nhap"] = pd.to_datetime(work["NGAY_HAP_DATE"], errors="coerce")
    else:
        work["__nhap"] = work.get("NGAY_HAP", pd.Series(dtype="object")).apply(_parse_datetime_safe)
    return stable_sort_dataframe(
        work,
        primary_columns=["__exp", "__nhap", "id"],
        fallback_name_columns=["TEN_DUNG_CU"],
    )


def render_tab_kho_dung_cu(danh_sach_ten: List[str]) -> None:
    st.header("🏥 QUẢN LÝ DỤNG CỤ & TIỆT TRÙNG")
    df_dm = _normalize_kho_columns(lay_du_lieu_supabase("kho_danhmuc"))
    df_nk = _normalize_kho_columns(lay_du_lieu_supabase("kho_nhatky"))
    df_dm = _ensure_tool_name_column(df_dm)
    df_nk = _ensure_tool_name_column(df_nk)
    if df_dm.empty:
        st.warning("⚠️ Lỗi hệ thống: Không có dữ liệu kho_danhmuc.")
        return

    df_dm["TỒN SẴN SÀNG"] = pd.to_numeric(df_dm.get("TỒN SẴN SÀNG", 0), errors="coerce").fillna(0).astype(int)
    df_dm["ĐANG HẤP"] = pd.to_numeric(df_dm.get("ĐANG HẤP", 0), errors="coerce").fillna(0).astype(int)
    df_dm["_ORIGINAL_POS"] = range(len(df_dm))
    df_dm = stable_sort_dataframe(
        df_dm,
        primary_columns=["STT", "THỨ TỰ", "ORDER_INDEX", "_ORIGINAL_POS"],
        fallback_name_columns=["TÊN BỘ DỤNG CỤ"],
    )
    df_gui_hap_log = _normalize_kho_columns(lay_du_lieu_supabase("kho_gui_hap_log"))
    df_nhan_ve_log_raw = lay_du_lieu_supabase("kho_nhan_ve_log")
    df_nhan_ve_log = _build_receive_log_from_fifo(df_nhan_ve_log_raw)
    if df_nhan_ve_log.empty and not df_nhan_ve_log_raw.empty:
        df_nhan_ve_log = df_nhan_ve_log_raw.copy()
    df_batches = _normalize_batch_columns(lay_du_lieu_supabase("kho_lo_hap"))
    if "TÊN BỘ DỤNG CỤ" not in df_batches.columns and "TEN_DUNG_CU" in df_batches.columns:
        df_batches["TÊN BỘ DỤNG CỤ"] = df_batches["TEN_DUNG_CU"].astype(str)

    df_holding = df_nk[df_nk.get("TÌNH TRẠNG", "") == "Đang giữ"] if not df_nk.empty else pd.DataFrame()

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
    tool_name_col = "TÊN BỘ DỤNG CỤ" if "TÊN BỘ DỤNG CỤ" in df_dm.columns else ""
    if not tool_name_col:
        fallback_tool_cols = [c for c in ("TEN_DUNG_CU", "TOOL_NAME", "TÊN DỤNG CỤ") if c in df_dm.columns]
        if fallback_tool_cols:
            tool_name_col = fallback_tool_cols[0]
            df_dm["TÊN BỘ DỤNG CỤ"] = df_dm[tool_name_col].astype(str)
        else:
            st.warning("Không tìm thấy cột tên dụng cụ trong kho_danhmuc.")
            return
    map_label_to_name = dict(zip(df_dm["LABEL"], df_dm["TÊN BỘ DỤNG CỤ"]))
    tool_options = [str(x) for x in df_dm["LABEL"].tolist() if str(x).strip()]
    fefo_tool_priority = _build_fefo_tool_priority(df_batches)
    # Giữ thứ tự theo kho_danhmuc (ổn định), không reorder theo FEFO hay lượt ghi.

    t1, t2, t3, t4 = st.tabs(["🚀 LẤY & CHỐT", "📤 GỬI TT", "📥 NHẬN VỀ", "📊 BÁO CÁO"])

    with t1:
        c1, c2 = st.columns([1.2, 1.8])
        with c1:
            st.subheader("📍 Lấy dụng cụ")
            selected_take_labels = st.multiselect(
                "Chọn dụng cụ (có thể chọn nhiều):",
                options=tool_options,
                key="fefo_tool_multi_take",
            )
            selected_tools = [map_label_to_name.get(label, label) for label in selected_take_labels]

            if not selected_tools:
                st.caption("Chọn ít nhất 1 dụng cụ để lấy.")
            else:
                with st.form("f_bulk_take_tools", clear_on_submit=False):
                    nv_l = st.selectbox("Người lấy:", options=danh_sach_ten, key="bulk_take_nv")
                    bulk_plan: List[Dict[str, Any]] = []

                    for tool_selected in selected_tools:
                        fefo_df = _get_fefo_batches_from_cache(df_batches, tool_selected)
                        if fefo_df.empty or not {"TEN_DUNG_CU", "SO_LUONG"}.issubset(fefo_df.columns):
                            st.warning(f"⚠️ `{tool_selected}` chưa có lô ready trong kho_lo_hap.")
                            continue

                        fefo_df = fefo_df.copy()
                        fefo_df["SO_LUONG"] = pd.to_numeric(fefo_df["SO_LUONG"], errors="coerce").fillna(0).astype(int)
                        fefo_df = fefo_df[fefo_df["SO_LUONG"] > 0].copy()
                        if fefo_df.empty:
                            st.warning(f"⚠️ `{tool_selected}` không còn số lượng khả dụng theo lô.")
                            continue

                        if "HAN_DUNG_DATE" in fefo_df.columns:
                            fefo_df["__exp"] = pd.to_datetime(fefo_df["HAN_DUNG_DATE"], errors="coerce")
                        else:
                            fefo_df["__exp"] = fefo_df.get("HAN_DUNG", pd.Series(dtype="object")).apply(_parse_datetime_safe)
                        if "NGAY_HAP_DATE" in fefo_df.columns:
                            fefo_df["__nhap"] = pd.to_datetime(fefo_df["NGAY_HAP_DATE"], errors="coerce")
                        else:
                            fefo_df["__nhap"] = fefo_df.get("NGAY_HAP", pd.Series(dtype="object")).apply(_parse_datetime_safe)
                        fefo_df = stable_sort_dataframe(
                            fefo_df,
                            primary_columns=["__exp", "__nhap", "id"],
                            fallback_name_columns=["TEN_DUNG_CU"],
                        )

                        st.markdown(f"**{tool_selected} — gợi ý FEFO theo hạn dùng**")
                        first_row = fefo_df.iloc[0]
                        sug_ngay_hap = first_row.get("NGAY_HAP_DATE", first_row.get("NGAY_HAP", ""))
                        sug_han_dung = first_row.get("HAN_DUNG_DATE", first_row.get("HAN_DUNG", ""))
                        st.info(f"Ưu tiên dùng lô ngày {sug_ngay_hap} - hết hạn {sug_han_dung}")

                        # --- SỬA TỐI THIỂU: BỎ CHỌN LÔ, THAY BẰNG NHẬP SỐ LƯỢNG ---
                        qty_take = st.number_input(
                            f"Số lượng lấy `{tool_selected}`:",
                            min_value=1,
                            value=1,
                            step=1,
                            key=f"bulk_qty_{tool_selected}",
                        )
                        # Lưu cả df lô đã sắp xếp vào bulk_plan để dùng lúc Submit
                        bulk_plan.append({
                            "tool_name": tool_selected,
                            "fefo_df": fefo_df, 
                            "qty": int(qty_take),
                        })
                        # --- HẾT PHẦN SỬA ---

                    
                    if st.form_submit_button("XÁC NHẬN LẤY TẤT CẢ"):
                        if not bulk_plan:
                            st.error("Không có mục nào hợp lệ.")
                        else:
                            for item in bulk_plan:
                                tool_selected = item["tool_name"]
                                qty_can_lay = item["qty"]
                                fefo_df = item["fefo_df"]
                                
                                # Tự động trừ lô FEFO
                                for _, row in fefo_df.iterrows():
                                    if qty_can_lay <= 0: break
                                    
                                    batch_id = int(row.get("id", row.get("ID")))
                                    sl_lo_kha_dung = int(row["SO_LUONG"])
                                    sl_lay_lo_nay = min(qty_can_lay, sl_lo_kha_dung)
                                    
                                    if sl_lay_lo_nay > 0:
                                        deduct_batch(batch_id, sl_lay_lo_nay)
                                        qty_can_lay -= sl_lay_lo_nay
                                
                                # Ghi log và trừ tồn kho (GIỮ NGUYÊN LOGIC CŨ CỦA ANH)
                                log_usage(tool_selected, None, item["qty"], nv_l)
                                ghi_du_lieu_supabase("kho_nhatky", [{
                                    "NGÀY GIỜ": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                                    "NHÂN VIÊN": nv_l, "HÀNH ĐỘNG": "LẤY",
                                    "TÊN BỘ DỤNG CỤ": tool_selected, "SỐ LƯỢNG": item["qty"],
                                    "TÌNH TRẠNG": "Đang giữ"
                                }])
                                
                                selected_dm = df_dm[df_dm["TÊN BỘ DUNG CỤ"] == tool_selected]
                                if not selected_dm.empty:
                                    d_id = int(selected_dm.iloc[0].get("id", selected_dm.iloc[0].get("ID")))
                                    cur_ton = int(pd.to_numeric(selected_dm.iloc[0].get("TỒN SẴN SÀNG", 0)))
                                    ghi_du_lieu_supabase("kho_danhmuc", [{"id": d_id, "TỒN SẴN SÀNG": max(0, cur_ton - item["qty"])}])
                            
                            st.rerun()
            with st.expander("Chế độ khẩn: Bỏ qua lô hấp", expanded=False):
                st.caption(
                    "Dùng khi dữ liệu lô hấp bị thiếu/sai. Hệ thống vẫn trừ tồn và ghi nhật ký, "
                    "nhưng sẽ không trừ theo lô FEFO."
                )
                tool_selected_label = st.selectbox(
                    "Chọn dụng cụ (khẩn):",
                    options=tool_options,
                    key="fefo_tool_select_emergency",
                )
                tool_selected = map_label_to_name.get(tool_selected_label, tool_selected_label)
                selected_dm = df_dm[df_dm["TÊN BỘ DỤNG CỤ"] == tool_selected]
                ton_san_sang = 0
                if not selected_dm.empty:
                    ton_san_sang = int(pd.to_numeric(selected_dm.iloc[0].get("TỒN SẴN SÀNG", 0), errors="coerce"))

                if ton_san_sang <= 0:
                    st.info("Không thể xuất khẩn vì tồn sẵn sàng hiện tại bằng 0.")
                else:
                    with st.form("f_lay_bo_qua_lo", clear_on_submit=True):
                        nv_l_manual = st.selectbox("Người lấy (khẩn):", options=danh_sach_ten, key="nv_take_manual")
                        qty_take_manual = st.number_input(
                            "Số lượng lấy (khẩn):",
                            min_value=1,
                            max_value=int(ton_san_sang),
                            value=1,
                            step=1,
                            key="qty_take_manual",
                        )
                        if st.form_submit_button("XÁC NHẬN LẤY (BỎ QUA LÔ)"):
                            r_dm = selected_dm.iloc[0].to_dict() if not selected_dm.empty else {}
                            d_id = r_dm.get("id", r_dm.get("ID"))
                            if d_id is None:
                                st.error("Không tìm thấy ID dụng cụ trong kho_danhmuc.")
                            else:
                                ok_log_usage = log_usage(tool_selected, None, int(qty_take_manual), nv_l_manual)
                                ok_nhatky = ghi_du_lieu_supabase(
                                    "kho_nhatky",
                                    [
                                        {
                                            "NGÀY GIỜ": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                                            "NHÂN VIÊN": nv_l_manual,
                                            "HÀNH ĐỘNG": "LẤY (BỎ QUA LÔ)",
                                            "TÊN BỘ DỤNG CỤ": tool_selected,
                                            "SỐ LƯỢNG": int(qty_take_manual),
                                            "TÌNH TRẠNG": "Đang giữ",
                                        }
                                    ],
                                )
                                ok_dm = ghi_du_lieu_supabase(
                                    "kho_danhmuc",
                                    [{"id": int(d_id), "TỒN SẴN SÀNG": int(ton_san_sang) - int(qty_take_manual)}],
                                )
                                if ok_log_usage and ok_nhatky and ok_dm:
                                    st.rerun()
                                else:
                                    st.error("Xuất khẩn chưa hoàn tất. Vui lòng kiểm tra kết nối và quyền ghi dữ liệu.")
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
                        txt_tt = "🟢 Đang giữ" if row.get("TÌNH TRẠNG", "") == "Đang giữ" else "🟡 Chờ hấp"
                        ci.write(f"{txt_tt} | **{row.get('NHÂN VIÊN', '')}**\n\n{row.get('TÊN BỘ DỤNG CỤ', row.get('TEN_DUNG_CU', row.get('TOOL_NAME', '')))}")
                        n_qty = cq.number_input("Số", 0, 100, int(row.get("SỐ LƯỢNG", 0)), key=f"ed_{index}")
                        if cb.button("CHỐT", key=f"btn_{index}"):
                            tool_name_for_row = row.get("TÊN BỘ DỤNG CỤ", row.get("TEN_DUNG_CU", row.get("TOOL_NAME", "")))
                            r_dm = df_dm[df_dm["TÊN BỘ DỤNG CỤ"] == tool_name_for_row].iloc[0].to_dict()
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
        ngay_gui = c_date.date_input("Ngày gửi:", value=datetime.now(), key="ngay_gui_hap")
        cho_g = df_nk[df_nk["TÌNH TRẠNG"] == "Chờ đi hấp"] if not df_nk.empty else pd.DataFrame()
        cho_g = stable_sort_dataframe(
            cho_g,
            primary_columns=["NGÀY GIỜ", "id"],
            fallback_name_columns=["TÊN BỘ DỤNG CỤ", "NHÂN VIÊN"],
        )
        st.write("---")
        st.write("➕ **Gửi thêm đồ khác**")
        selected_labels = st.multiselect(
            "Chọn nhiều bộ dụng cụ:",
            options=tool_options,
            key="add_g_multi",
        )
        selected_tools = [map_label_to_name.get(label, label) for label in selected_labels]
        qty_by_tool: Dict[str, int] = {}
        if selected_tools:
            qty_cols = st.columns(2)
            for idx, tool_name in enumerate(selected_tools):
                with qty_cols[idx % 2]:
                    qty_by_tool[tool_name] = st.number_input(
                        f"SL {tool_name}:",
                        min_value=1,
                        max_value=100,
                        value=1,
                        step=1,
                        key=f"add_s_{tool_name}",
                    )
        if st.button("THÊM"):
            if not selected_tools:
                st.warning("Vui lòng chọn ít nhất 1 bộ dụng cụ.")
            else:
                for tool_name in selected_tools:
                    qty_val = int(qty_by_tool.get(tool_name, 1))
                    ghi_du_lieu_supabase(
                        "kho_nhatky",
                        [
                            {
                                "NGÀY GIỜ": ngay_gui.strftime("%d/%m/%Y"),
                                "NHÂN VIÊN": "Hệ thống",
                                "HÀNH ĐỘNG": "GỬI THÊM",
                                "TÊN BỘ DỤNG CỤ": tool_name,
                                "SỐ LƯỢNG": qty_val,
                                "TÌNH TRẠNG": "Chờ đi hấp",
                            }
                        ],
                    )
                    r_dm_t = df_dm[df_dm["TÊN BỘ DỤNG CỤ"] == tool_name].iloc[0].to_dict()
                    id_t = r_dm_t.get("id", r_dm_t.get("ID"))
                    ghi_du_lieu_supabase(
                        "kho_danhmuc",
                        [{"id": int(id_t), "TỒN SẴN SÀNG": int(r_dm_t["TỒN SẴN SÀNG"]) - qty_val}],
                    )
                st.rerun()

        if not cho_g.empty:
            display_cho_g = cho_g.copy()
            if {"NHÂN VIÊN", "TÊN BỘ DỤNG CỤ", "SỐ LƯỢNG"}.issubset(display_cho_g.columns):
                display_cho_g = display_cho_g.groupby(["NHÂN VIÊN", "TÊN BỘ DỤNG CỤ"], as_index=False)["SỐ LƯỢNG"].sum()
            st.dataframe(display_cho_g[["NHÂN VIÊN", "TÊN BỘ DỤNG CỤ", "SỐ LƯỢNG"]], use_container_width=True)
            if st.button("🚀 XÁC NHẬN GỬI TOÀN BỘ"):
                ds_sum = cho_g.groupby("TÊN BỘ DỤNG CỤ")["SỐ LƯỢNG"].sum().reset_index()
                time_sent = datetime.now()
                sent_rows: List[Dict[str, Any]] = []
                for _, row in ds_sum.iterrows():
                    tool_name_for_row = row.get("TÊN BỘ DỤNG CỤ", row.get("TEN_DUNG_CU", row.get("TOOL_NAME", "")))
                    r_dm_u = df_dm[df_dm["TÊN BỘ DỤNG CỤ"] == tool_name_for_row].iloc[0].to_dict()
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
                    sent_rows.append(
                        {
                            "TOOL_NAME": row["TÊN BỘ DỤNG CỤ"],
                            "QUANTITY_SENT": int(row["SỐ LƯỢNG"]),
                            "TIMESTAMP_SENT": time_sent.strftime("%d/%m/%Y %H:%M:%S"),
                        }
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
                if not log_tools_sent_for_sterilization(sent_rows):
                    st.warning("Đã gửi đi hấp nhưng chưa ghi được log kho_gui_hap_log.")
                st.success("Đã gửi đi!")
                st.rerun()
        st.markdown("### Lịch sử gửi hấp theo ngày")
        if not df_gui_hap_log.empty:
            view_send = df_gui_hap_log.copy()
            if "TIMESTAMP_SENT" in view_send.columns:
                view_send["__ts"] = view_send["TIMESTAMP_SENT"].apply(_parse_datetime_safe)
            elif "NGÀY GIỜ" in view_send.columns:
                view_send["__ts"] = view_send["NGÀY GIỜ"].apply(_parse_datetime_safe)
            else:
                view_send["__ts"] = pd.NaT
            view_send["__date"] = view_send["__ts"].dt.date
            # `st.date_input` trả về `datetime.date`
            view_send = view_send[view_send["__date"] == ngay_gui].copy()
            view_send = stable_sort_dataframe(
                view_send,
                primary_columns=["__ts", "ORDER_INDEX", "id"],
                fallback_name_columns=["TOOL_NAME", "TÊN BỘ DỤNG CỤ"],
            )
            show_cols = [c for c in ["TOOL_NAME", "QUANTITY_SENT", "TIMESTAMP_SENT"] if c in view_send.columns]
            if not show_cols:
                show_cols = [c for c in ["TÊN BỘ DỤNG CỤ", "SỐ LƯỢNG", "NGÀY GIỜ"] if c in view_send.columns]
            if view_send.empty:
                st.caption("Không có dữ liệu gửi hấp trong ngày đã chọn.")
            else:
                view_send = _group_tool_log_view(
                    view_send,
                    tool_cols=["TOOL_NAME", "TÊN BỘ DỤNG CỤ"],
                    qty_cols=["QUANTITY_SENT", "SỐ LƯỢNG"],
                    date_cols=["TIMESTAMP_SENT", "NGÀY GIỜ"],
                )
                show_cols = [c for c in ["TOOL_NAME", "TÊN BỘ DỤNG CỤ", "QUANTITY_SENT", "SỐ LƯỢNG", "TIMESTAMP_SENT", "NGÀY GIỜ"] if c in view_send.columns]
                st.dataframe(view_send[show_cols], use_container_width=True, hide_index=True)
        else:
            df_send_fallback = _build_send_log_from_nhatky(df_nk)
            if df_send_fallback.empty:
                st.caption("Chưa có dữ liệu gửi hấp.")
            else:
                view_fb = df_send_fallback.copy()
                view_fb["__ts"] = view_fb["TIMESTAMP_SENT"].apply(_parse_datetime_safe)
                view_fb["__date"] = view_fb["__ts"].dt.date
                # `st.date_input` trả về `datetime.date`
                view_fb = view_fb[view_fb["__date"] == ngay_gui].copy()
                if view_fb.empty:
                    st.caption("Không có dữ liệu gửi hấp trong ngày đã chọn.")
                else:
                    view_fb = _group_tool_log_view(
                        view_fb,
                        tool_cols=["TOOL_NAME"],
                        qty_cols=["QUANTITY_SENT"],
                        date_cols=["TIMESTAMP_SENT"],
                    )
                    st.dataframe(
                        view_fb[[c for c in ["TOOL_NAME", "QUANTITY_SENT", "TIMESTAMP_SENT"] if c in view_fb.columns]],
                        use_container_width=True,
                        hide_index=True,
                    )

    with t3:
        st.subheader("📥 Nhận về kho")
        ngay_nhan = st.date_input("Ngày hấp/nhận:", value=datetime.now(), key="ngay_nhan_kho")

        df_dang_hap = df_dm.copy()
        df_dang_hap["ĐANG HẤP"] = pd.to_numeric(df_dang_hap.get("ĐANG HẤP", 0), errors="coerce").fillna(0).astype(int)
        df_dang_hap = df_dang_hap[df_dang_hap["ĐANG HẤP"] > 0].copy()
        df_dang_hap = stable_sort_dataframe(
            df_dang_hap,
            primary_columns=["GHI CHÚ", "STT", "THỨ TỰ", "ORDER_INDEX"],
            fallback_name_columns=["TÊN BỘ DỤNG CỤ"],
        )

        if df_dang_hap.empty:
            st.info("Hiện không có dụng cụ nào đang hấp.")
        else:
            st.markdown("### Danh sách đang hấp (xác nhận nhận về)")
            with st.form("f_nhan_ve_bulk", clear_on_submit=True):
                recv_rows: List[Dict[str, Any]] = []
                cols = st.columns(2)
                for idx, (_, r) in enumerate(df_dang_hap.iterrows()):
                    with cols[idx % 2]:
                        ten = str(r.get("TÊN BỘ DỤNG CỤ", "")).strip()
                        if not ten:
                            continue
                        dang_hap_qty = int(r.get("ĐANG HẤP", 0))
                        st.write(f"**{ten}** (Đang hấp: {dang_hap_qty})")
                        qty_recv = st.number_input(
                            "Số lượng nhận về:",
                            min_value=0,
                            max_value=dang_hap_qty,
                            value=0,
                            step=1,
                            key=f"recv_{ten}",
                        )
                        recv_rows.append(
                            {
                                "ten": ten,
                                "qty": int(qty_recv),
                            }
                        )

                if st.form_submit_button("🚀 XÁC NHẬN NHẬN VỀ (THEO NGÀY ĐÃ CHỌN)", use_container_width=True):
                    to_process = [x for x in recv_rows if int(x["qty"]) > 0]
                    if not to_process:
                        st.warning("Bạn chưa nhập số lượng nhận về cho dụng cụ nào.")
                    else:
                        any_failed = False
                        for item in to_process:
                            m_v = item["ten"]
                            s_v = int(item["qty"])
                            sel = df_dm[df_dm["TÊN BỘ DỤNG CỤ"] == m_v]
                            if sel.empty:
                                any_failed = True
                                st.error(f"Không tìm thấy `{m_v}` trong kho_danhmuc.")
                                continue
                            r_dm_n = sel.iloc[0].to_dict()
                            id_n = r_dm_n.get("id", r_dm_n.get("ID"))
                            if id_n is None:
                                any_failed = True
                                st.error(f"Không tìm thấy ID của `{m_v}`.")
                                continue

                            ngay_het_han = ngay_nhan + timedelta(days=KHO_EXPIRY_DAYS)
                            ok_dm = ghi_du_lieu_supabase(
                                "kho_danhmuc",
                                [
                                    {
                                        "id": int(id_n),
                                        "TỒN SẴN SÀNG": int(r_dm_n["TỒN SẴN SÀNG"]) + s_v,
                                        "ĐANG HẤP": max(0, int(r_dm_n["ĐANG HẤP"]) - s_v),
                                        "GHI CHÚ": ngay_nhan.strftime("%d/%m/%Y"),
                                    }
                                ],
                            )
                            ok_batch = insert_batch(
                                ten_dung_cu=m_v,
                                ngay_hap=ngay_nhan,
                                so_luong=int(s_v),
                                han_dung=ngay_het_han,
                            )
                            ok_log = log_tools_received_with_expiry(
                                [
                                    {
                                        # Dùng tên cột lowercase đúng schema Supabase hiện tại
                                        "tool_name": m_v,
                                        "quantity": int(s_v),
                                        "remaining_qty": int(s_v),
                                        "date_received": ngay_nhan.isoformat(),
                                        "date_received_date": ngay_nhan.isoformat(),
                                        "expiry_date": ngay_het_han.isoformat(),
                                        "expiry_date_date": ngay_het_han.isoformat(),
                                    }
                                ]
                            )
                            if not (ok_dm and ok_batch and ok_log):
                                any_failed = True
                                st.error(f"Nhận về `{m_v}` chưa hoàn tất.")

                        if not any_failed:
                            st.rerun()
                st.markdown("### Lịch sử nhận về theo ngày")
                if df_nhan_ve_log.empty:
                    st.caption("Chưa có dữ liệu nhận về.")
                else:
                    view_recv = df_nhan_ve_log.copy()
                    
                    # 1. Tự động tìm tên cột (Bỏ qua viết hoa/thường)
                    col_ten = next(
                        (c for c in ["tool_name", "TOOL_NAME", "TÊN BỘ DỤNG CỤ", "TEN_DUNG_CU", "TÊN DỤNG CỤ"] if c in view_recv.columns),
                        None,
                    )
                    col_sl = next(
                        (c for c in ["quantity", "QUANTITY", "SỐ LƯỢNG", "SO_LUONG"] if c in view_recv.columns),
                        None,
                    )
                    col_sl_con = next(
                        (c for c in ["remaining_qty", "REMAINING_QTY", "SL_CÒN", "SL CON", "SO_LUONG_CON"] if c in view_recv.columns),
                        None,
                    )
                    col_ngay = next(
                        (c for c in ["date_received", "DATE_RECEIVED", "date_received_date", "DATE_RECEIVED_DATE", "__rcv"] if c in view_recv.columns),
                        None,
                    )
                    col_han = next(
                        (c for c in ["expiry_date", "EXPIRY_DATE", "expiry_date_date", "EXPIRY_DATE_DATE", "__exp"] if c in view_recv.columns),
                        None,
                    )

                    if col_ten is None or col_sl is None or col_ngay is None:
                        st.warning("Không tìm thấy cột dữ liệu nhận về cần thiết (tên, số lượng hoặc ngày nhận).")
                        return

                    if col_ngay == "__rcv":
                        view_recv["__d"] = view_recv["__rcv"].dt.date
                    else:
                        view_recv["__d"] = view_recv[col_ngay].apply(_parse_datetime_safe).dt.date
                    view_recv = view_recv[view_recv["__d"] == ngay_nhan].copy()
                    
                    if view_recv.empty:
                        st.caption("Không có dữ liệu nhận về trong ngày đã chọn.")
                    else:
                        # 2. XỬ LÝ VIẾT HOA/THƯỜNG Ở ĐÂY:
                        # Chuyển tên về viết thường và cắt khoảng trắng trước khi gộp
                        view_recv["_TEMP_NAME"] = view_recv[col_ten].astype(str).str.strip().str.lower()

                        # 3. Gộp nhóm
                        agg_map: Dict[str, str] = {col_sl: "sum", col_ngay: "first"}
                        if col_sl_con is not None and col_sl_con in view_recv.columns:
                            agg_map[col_sl_con] = "sum"
                        if col_han is not None and col_han in view_recv.columns:
                            agg_map[col_han] = "first"

                        view_recv_grouped = view_recv.groupby("_TEMP_NAME", as_index=False).agg(agg_map)

                        # Đổi lại tên cột hiển thị cho đẹp
                        view_recv_grouped = view_recv_grouped.rename(columns={"_TEMP_NAME": col_ten})

                        # 4. Hiển thị
                        st.dataframe(
                            view_recv_grouped[[col_ten, col_sl, col_sl_con, col_ngay, col_han]],
                            use_container_width=True,
                            hide_index=True,
                        )


    with t4:
        st.subheader("📊 Báo cáo kho")
        st.markdown("### Cơ số kho hiện tại")
        if "TÊN BỘ DỤNG CỤ" not in df_dm.columns:
            fallback_tool_cols = [c for c in ("TEN_DUNG_CU", "TOOL_NAME", "TÊN DỤNG CỤ") if c in df_dm.columns]
            if fallback_tool_cols:
                df_dm["TÊN BỘ DỤNG CỤ"] = df_dm[fallback_tool_cols[0]].astype(str)
        # Ưu tiên hiển thị cột CƠ SỐ nếu Supabase đã có sẵn.
        has_co_so = "CƠ SỐ" in df_dm.columns
        has_ton_san_sang = "TỒN SẴN SÀNG" in df_dm.columns
        has_dang_hap = "ĐANG HẤP" in df_dm.columns

        show_4_cols = st.checkbox(
            "Hiện thêm cột Tồn sẵn sàng (gọn trên điện thoại vẫn xem được)",
            value=False,
            key="kho_report_show4",
        )

        base_cols = ["TÊN BỘ DỤNG CỤ"]
        if has_co_so:
            base_cols.append("CƠ SỐ")
        elif has_ton_san_sang:
            # fallback nếu bảng chưa có cột CƠ SỐ
            base_cols.append("TỒN SẴN SÀNG")
        if has_dang_hap:
            base_cols.append("ĐANG HẤP")

        if show_4_cols and has_ton_san_sang and "TỒN SẴN SÀNG" not in base_cols:
            base_cols.insert(2 if len(base_cols) >= 2 else 1, "TỒN SẴN SÀNG")

        base_cols = [c for c in base_cols if c in df_dm.columns]
        if not base_cols:
            st.caption("Không tìm thấy cột để hiển thị báo cáo kho.")
        else:
            view_dm = df_dm[base_cols].copy()
            # Gộp các dòng cùng tên dụng cụ nếu Supabase trả về nhiều dòng giống nhau.
            if "TÊN BỘ DỤNG CỤ" in view_dm.columns:
                sum_cols = [c for c in view_dm.columns if c not in ("TÊN BỘ DỤNG CỤ", "GHI CHÚ") and pd.api.types.is_numeric_dtype(view_dm[c].dtype)]
                if sum_cols:
                    agg_map: Dict[str, Any] = {"TÊN BỘ DỤNG CỤ": "first"}
                    agg_map.update({c: "sum" for c in sum_cols})
                    view_dm = view_dm.groupby("TÊN BỘ DỤNG CỤ", as_index=False).agg(agg_map)
                    original_order = {name: idx for idx, name in enumerate(df_dm["TÊN BỘ DỤNG CỤ"].astype(str).tolist())}
                    view_dm["_ORIG_ORDER"] = view_dm["TÊN BỘ DỤNG CỤ"].astype(str).map(original_order)
                    view_dm = view_dm.sort_values(by=["_ORIG_ORDER"], kind="stable").drop(columns=["_ORIG_ORDER"], errors="ignore")

            # Nếu dùng fallback "TỒN SẴN SÀNG" thay cho CƠ SỐ thì đổi nhãn hiển thị cho đúng ý nghĩa
            if (not has_co_so) and ("TỒN SẴN SÀNG" in view_dm.columns) and (not show_4_cols):
                view_dm = view_dm.rename(columns={"TỒN SẴN SÀNG": "CƠ SỐ"})

            st.dataframe(
                view_dm,
                use_container_width=True,
                hide_index=True,
                height=360,
                column_config={
                    "TÊN BỘ DỤNG CỤ": st.column_config.TextColumn("DANH MỤC", width="medium"),
                    "CƠ SỐ": st.column_config.NumberColumn("CƠ SỐ", width="small"),
                    "TỒN SẴN SÀNG": st.column_config.NumberColumn("TỒN", width="small"),
                    "ĐANG HẤP": st.column_config.NumberColumn("ĐANG HẤP", width="small"),
                },
            )
        st.markdown("---")
        st.markdown("### Báo cáo theo lô hấp")
        st.caption("Báo cáo theo lô hấp (FEFO: hạn gần nhất trước).")
        if not df_batches.empty and {"TEN_DUNG_CU", "SO_LUONG"}.issubset(df_batches.columns):
            report_df = df_batches.copy()
            report_df["SO_LUONG"] = pd.to_numeric(report_df["SO_LUONG"], errors="coerce").fillna(0).astype(int)
            if "TRANG_THAI" not in report_df.columns:
                report_df["TRANG_THAI"] = report_df["SO_LUONG"].apply(lambda x: "used" if int(x) <= 0 else "ready")
            if "HAN_DUNG_DATE" in report_df.columns:
                report_df["__exp"] = pd.to_datetime(report_df["HAN_DUNG_DATE"], errors="coerce")
            else:
                report_df["__exp"] = report_df.get("HAN_DUNG", pd.Series(dtype="object")).apply(_parse_datetime_safe)
            if "NGAY_HAP_DATE" in report_df.columns:
                report_df["__nhap"] = pd.to_datetime(report_df["NGAY_HAP_DATE"], errors="coerce")
            else:
                report_df["__nhap"] = report_df.get("NGAY_HAP", pd.Series(dtype="object")).apply(_parse_datetime_safe)
            report_df = stable_sort_dataframe(
                report_df,
                primary_columns=["__exp", "__nhap", "id"],
                fallback_name_columns=["TEN_DUNG_CU"],
            )
            report_df = report_df.reset_index(drop=True)
            report_df["PRIORITY"] = report_df.index.map(_fefo_priority_label)
            report_df["PRIORITY"] = report_df["PRIORITY"].apply(_fefo_priority_badge)
            report_df["NGAY_HAP_SHOW"] = report_df.get("NGAY_HAP_DATE", report_df.get("NGAY_HAP", ""))
            report_df["HAN_DUNG_SHOW"] = report_df.get("HAN_DUNG_DATE", report_df.get("HAN_DUNG", ""))
            st.dataframe(
                report_df[["TEN_DUNG_CU", "NGAY_HAP_SHOW", "SO_LUONG", "HAN_DUNG_SHOW", "TRANG_THAI", "PRIORITY"]],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.caption("Chưa có dữ liệu lô hấp trong kho_lo_hap.")
