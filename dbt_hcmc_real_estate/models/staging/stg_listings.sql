{{
  config(
    materialized='view'
  )
}}

/*
===============================================================================
Model: stg_listings
Layer: Staging (Medallion: Bronze -> Silver)
Description:
    Tầng Staging đọc dữ liệu thô từ nguồn `raw_listings` (Google BigQuery),
    thực hiện chuẩn hóa dữ liệu, bóc tách thuộc tính từ chuỗi văn bản (Regex),
    và ép kiểu dữ liệu chuẩn mực phục vụ tầng Marts và Machine Learning.
===============================================================================
*/

with source_data as (

    select * from {{ source('raw_data', 'raw_listings') }}

),

parsed_and_cleaned as (

    select
        -- 1. Khóa định danh & Metadata
        cast(link as string)                                            as link,
        cast(link as string)                                            as listing_url,
        coalesce(
            cast(ma_tin as string),
            regexp_extract(link, r'pr([0-9]+)')
        )                                                               as listing_id,
        cast(crawled_at as string)                                      as crawled_at,
        nullif(trim(cast(loai_tin as string)), '')                      as listing_type,
        nullif(trim(cast(ngay_dang as string)), '')                     as posted_date,
        nullif(trim(cast(ngay_het_han as string)), '')                  as expired_date,

        -- 2. Thông tin bài đăng & văn bản
        nullif(trim(cast(tieu_de as string)), '')                       as title,
        nullif(trim(cast(mo_ta as string)), '')                         as description,

        -- 3. Địa chỉ & Phân cấp hành chính (Đường, Phường/Xã, Quận)
        trim(cast(dia_chi as string))                                   as raw_address,

        -- Trích xuất Tên Đường / Phố
        trim(regexp_extract(dia_chi, r'(?:Đường|Phố)\s+([^,]+)'))        as street,

        -- Trích xuất Phường / Xã / Thị trấn
        trim(regexp_extract(dia_chi, r'(?:Phường|Xã|Thị trấn)\s+([^,]+)')) as ward,

        -- Trích xuất Quận / Huyện / Thành phố
        trim(regexp_extract(dia_chi, r'(?:Quận|Huyện|Thành phố)\s+([^,]+)')) as district,

        -- 4. Tọa độ địa lý GPS
        safe_cast(latitude as float64)                                  as latitude,
        safe_cast(longitude as float64)                                 as longitude,

        -- 5. Diện tích đất / sàn (m²)
        safe_cast(
            regexp_extract(replace(dien_tich, ',', '.'), r'([0-9]+[.]?[0-9]*)')
            as float64
        )                                                               as area_m2,

        -- 6. Chuẩn hóa Khoảng giá về đơn vị tỷ VNĐ
        case
            -- TH1: Đơn vị 'tỷ' (ví dụ: '16,9 tỷ', '10.5 tỷ')
            when lower(khoang_gia) like '%tỷ%' or lower(khoang_gia) like '%ty%'
                then safe_cast(regexp_extract(replace(khoang_gia, ',', '.'), r'([0-9]+[.]?[0-9]*)') as float64)

            -- TH2: Đơn vị 'triệu/m²' -> quy đổi = (giá_m2 * diện_tích) / 1000
            when (lower(khoang_gia) like '%triệu/m%' or lower(khoang_gia) like '%trieu/m%')
                 and safe_cast(regexp_extract(replace(dien_tich, ',', '.'), r'([0-9]+[.]?[0-9]*)') as float64) > 0
                then (
                    safe_cast(regexp_extract(replace(khoang_gia, ',', '.'), r'([0-9]+[.]?[0-9]*)') as float64)
                    * safe_cast(regexp_extract(replace(dien_tich, ',', '.'), r'([0-9]+[.]?[0-9]*)') as float64)
                ) / 1000.0

            -- TH3: Đơn vị 'triệu' tổng thể (ví dụ: '850 triệu', '950 triệu')
            when lower(khoang_gia) like '%triệu%' or lower(khoang_gia) like '%trieu%'
                then safe_cast(regexp_extract(replace(khoang_gia, ',', '.'), r'([0-9]+[.]?[0-9]*)') as float64) / 1000.0

            -- TH4: Thỏa thuận hoặc không rõ -> NULL
            else null
        end                                                             as price_billion_vnd,

        -- 7. Thông số kỹ thuật nhà đất
        safe_cast(regexp_extract(so_tang, r'([0-9]+)') as int64)        as floors,
        safe_cast(regexp_extract(so_phong_ngu, r'([0-9]+)') as int64)   as bedrooms,
        safe_cast(regexp_extract(so_phong_tam_ve_sinh, r'([0-9]+)') as int64) as bathrooms,
        safe_cast(
            regexp_extract(replace(duong_vao, ',', '.'), r'([0-9]+[.]?[0-9]*)')
            as float64
        )                                                               as lane_width_m,
        safe_cast(
            regexp_extract(replace(mat_tien, ',', '.'), r'([0-9]+[.]?[0-9]*)')
            as float64
        )                                                               as frontage_m,

        -- 8. Phân loại đường vào (Mặt tiền / Hẻm ô tô / Hẻm xe máy) từ Mô tả
        case
            when regexp_contains(lower(mo_ta), r'm[aặ]t ti[eề]n|nhà m[aặ]t ph[oố]|m[aặ]t ph[oố]')
                then 'Mặt tiền'
            when regexp_contains(lower(mo_ta), r'h[eẻ]m\s*(?:xe hơi|ô ?tô|2 xe quay đầu|hai xe|thông xe)|ô ?tô\s*vào\s*nhà|xe hơi\s*vào\s*nhà')
                then 'Hẻm ô tô'
            when regexp_contains(lower(mo_ta), r'h[eẻ]m')
                 and safe_cast(regexp_extract(replace(regexp_extract(lower(mo_ta), r'h[eẻ]m(?:\s+\w+)?\s*([\d,\.]+)\s*m'), ',', '.'), r'([0-9]+[.]?[0-9]*)') as float64) >= 4.0
                then 'Hẻm ô tô'
            when regexp_contains(lower(mo_ta), r'h[eẻ]m')
                then 'Hẻm xe máy'
            else 'Chưa xác định'
        end                                                             as lane_type,

        -- 9. Chuẩn hóa Pháp lý (Sổ hồng / Sổ đỏ / Không có)
        case
            when lower(phap_ly) like '%hồng%' or lower(phap_ly) like '%hong%' or lower(phap_ly) like '%shr%'
                then 'Sổ hồng'
            when lower(phap_ly) like '%sổ%' or lower(phap_ly) like '%đỏ%' or lower(phap_ly) like '%do%'
                 or lower(phap_ly) like '%sdc%' or lower(phap_ly) = 'sh'
                then 'Sổ đỏ'
            else 'Không có'
        end                                                             as legal_status,

        -- 10. Chuẩn hóa Tình trạng nội thất (Đầy đủ / Cơ bản / Trống - Nhà thô)
        case
            when lower(noi_that) like '%đầy đủ%' or lower(noi_that) like '%day du%' or lower(noi_that) like '%cao cấp%'
                then 'Đầy đủ'
            when lower(noi_that) like '%cơ bản%' or lower(noi_that) like '%co ban%'
                then 'Cơ bản'
            else 'Trống / Nhà thô'
        end                                                             as furniture_status,

        -- 11. Hướng nhà & Hướng ban công (giữ nguyên làm dữ liệu bổ trợ)
        nullif(trim(cast(huong_nha as string)), '')                     as house_direction,
        nullif(trim(cast(huong_ban_cong as string)), '')                as balcony_direction

    from source_data

)

select * from parsed_and_cleaned
