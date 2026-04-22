begin;

-- 1) Add new typed columns (backward-compatible, no drop old text columns).
alter table if exists public.kho_lo_hap
    add column if not exists ngay_hap_date date,
    add column if not exists han_dung_date date;

alter table if exists public.kho_xuat_log
    add column if not exists ngay_hap_date date,
    add column if not exists thoi_diem_xuat_ts timestamp;

-- Optional but recommended for receive log consistency.
alter table if exists public.kho_nhan_ve_log
    add column if not exists date_received_date date,
    add column if not exists expiry_date_date date;

-- 2) Safe text -> DATE/TIMESTAMP migration.
-- Rules:
-- - dd/mm/yyyy
-- - yyyy-mm-dd
-- - dd-mm-yyyy
-- - invalid formats => NULL (no hard failure)

update public.kho_lo_hap
set ngay_hap_date = case
        when coalesce(trim("NGAY_HAP"), '') = '' then null
        when trim("NGAY_HAP") ~ '^\d{2}/\d{2}/\d{4}$' then to_date(trim("NGAY_HAP"), 'DD/MM/YYYY')
        when trim("NGAY_HAP") ~ '^\d{4}-\d{2}-\d{2}$' then to_date(trim("NGAY_HAP"), 'YYYY-MM-DD')
        when trim("NGAY_HAP") ~ '^\d{2}-\d{2}-\d{4}$' then to_date(trim("NGAY_HAP"), 'DD-MM-YYYY')
        else null
    end
where ngay_hap_date is null;

update public.kho_lo_hap
set han_dung_date = case
        when coalesce(trim("HAN_DUNG"), '') = '' then null
        when trim("HAN_DUNG") ~ '^\d{2}/\d{2}/\d{4}$' then to_date(trim("HAN_DUNG"), 'DD/MM/YYYY')
        when trim("HAN_DUNG") ~ '^\d{4}-\d{2}-\d{2}$' then to_date(trim("HAN_DUNG"), 'YYYY-MM-DD')
        when trim("HAN_DUNG") ~ '^\d{2}-\d{2}-\d{4}$' then to_date(trim("HAN_DUNG"), 'DD-MM-YYYY')
        else null
    end
where han_dung_date is null;

update public.kho_xuat_log
set ngay_hap_date = case
        when coalesce(trim("NGAY_HAP"), '') = '' then null
        when trim("NGAY_HAP") ~ '^\d{2}/\d{2}/\d{4}$' then to_date(trim("NGAY_HAP"), 'DD/MM/YYYY')
        when trim("NGAY_HAP") ~ '^\d{4}-\d{2}-\d{2}$' then to_date(trim("NGAY_HAP"), 'YYYY-MM-DD')
        when trim("NGAY_HAP") ~ '^\d{2}-\d{2}-\d{4}$' then to_date(trim("NGAY_HAP"), 'DD-MM-YYYY')
        else null
    end
where ngay_hap_date is null;

update public.kho_xuat_log
set thoi_diem_xuat_ts = case
        when coalesce(trim("THOI_DIEM_XUAT"), '') = '' then null
        when trim("THOI_DIEM_XUAT") ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
            then to_timestamp(trim("THOI_DIEM_XUAT"), 'DD/MM/YYYY HH24:MI:SS')
        when trim("THOI_DIEM_XUAT") ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
            then to_timestamp(trim("THOI_DIEM_XUAT"), 'YYYY-MM-DD HH24:MI:SS')
        when trim("THOI_DIEM_XUAT") ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
            then to_timestamp(replace(trim("THOI_DIEM_XUAT"), 'T', ' '), 'YYYY-MM-DD HH24:MI:SS')
        else null
    end
where thoi_diem_xuat_ts is null;

update public.kho_nhan_ve_log
set date_received_date = case
        when coalesce(trim("DATE_RECEIVED"), '') = '' then null
        when trim("DATE_RECEIVED") ~ '^\d{2}/\d{2}/\d{4}$' then to_date(trim("DATE_RECEIVED"), 'DD/MM/YYYY')
        when trim("DATE_RECEIVED") ~ '^\d{4}-\d{2}-\d{2}$' then to_date(trim("DATE_RECEIVED"), 'YYYY-MM-DD')
        else null
    end
where date_received_date is null;

update public.kho_nhan_ve_log
set expiry_date_date = case
        when coalesce(trim("EXPIRY_DATE"), '') = '' then null
        when trim("EXPIRY_DATE") ~ '^\d{2}/\d{2}/\d{4}$' then to_date(trim("EXPIRY_DATE"), 'DD/MM/YYYY')
        when trim("EXPIRY_DATE") ~ '^\d{4}-\d{2}-\d{2}$' then to_date(trim("EXPIRY_DATE"), 'YYYY-MM-DD')
        else null
    end
where expiry_date_date is null;

-- 3) FEFO query performance and deterministic ordering.
create index if not exists idx_kho_lo_hap_fefo
    on public.kho_lo_hap ("TEN_DUNG_CU", han_dung_date asc, id asc);

commit;
