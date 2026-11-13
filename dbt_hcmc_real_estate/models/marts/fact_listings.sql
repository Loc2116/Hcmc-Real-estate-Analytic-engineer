{{
  config(
    materialized='table'
  )
}}

/*
===============================================================================
Model: fact_listings
Layer: Marts (Star Schema - Fact Table)
Description:
    Bảng sự kiện chính lưu trữ các tin đăng bất động sản TP.HCM đã lọc sạch.
    - Khóa chính: listing_id (phái sinh từ property_fingerprint)
    - Khóa ngoại: location_key (trỏ tới dim_location), property_spec_key (trỏ tới dim_property_specs)
    - Chỉ số đo lường (Measures): Giá (tỷ VNĐ), Diện tích (m²), Đơn giá (triệu VNĐ/m²),
      Số tầng, Số phòng, Chiều rộng hẻm, Mặt tiền, Số lượng môi giới cùng rao.
===============================================================================
*/

with intermediate as (

    select * from {{ ref('int_listings_deduplicated') }}

),

filtered_and_transformed as (

    select
        -- 1. Khóa chính & Khóa ngoại (Star Schema Keys)
        property_fingerprint                                            as listing_id,
        farm_fingerprint(
            concat(
                coalesce(district, 'UNKNOWN'), '|',
                coalesce(ward, 'UNKNOWN'), '|',
                coalesce(street, 'UNKNOWN')
            )
        )                                                               as location_key,
        farm_fingerprint(
            concat(
                coalesce(lane_type, 'UNKNOWN'), '|',
                coalesce(legal_status, 'UNKNOWN'), '|',
                coalesce(furniture_status, 'UNKNOWN')
            )
        )                                                               as property_spec_key,

        -- 2. Định danh & Dấu vết thời gian
        link,
        crawled_at,
        date(safe_cast(crawled_at as timestamp))                        as crawled_date,

        -- 3. Các chỉ số đo lường cốt lõi (Core Measures)
        price_billion_vnd,
        area_m2,
        round((price_billion_vnd * 1000.0) / area_m2, 2)                as price_per_m2,

        -- 4. Thông số kỹ thuật nhà đất
        floors,
        bedrooms,
        bathrooms,
        case
            when bedrooms is not null or bathrooms is not null
            then coalesce(bedrooms, 0) + coalesce(bathrooms, 0)
            else null
        end                                                             as total_rooms,
        lane_width_m,
        frontage_m,

        -- 5. Chỉ số thanh khoản & Cờ thiếu dữ liệu (Entity Resolution & MNAR Indicators)
        duplicate_broker_count,
        case
            when lane_type = 'Chưa xác định' then 1
            else 0
        end                                                             as is_lane_type_missing,
        case
            when lane_width_m is null then 1
            else 0
        end                                                             as is_lane_width_missing

    from intermediate

    where
        -- LỌC CHẤT LƯỢNG DỮ LIỆU & NGOẠI LAI (QUALITY & OUTLIER FILTERS):
        -- Bắt buộc phải có giá và diện tích hợp lệ
        price_billion_vnd is not null
        and price_billion_vnd > 0.05               -- Giá từ 50 triệu trở lên
        and area_m2 is not null
        and area_m2 between 5.0 and 2000.0         -- Diện tích từ 5m² đến 2000m²
        and ((price_billion_vnd * 1000.0) / area_m2) between 3.0 and 2500.0 -- Đơn giá từ 3tr đến 2.5 tỷ/m²

        -- Lọc ngoại lai Số tầng (nhà phố tại TP.HCM không quá 12 tầng)
        and (floors is null or floors between 1 and 12)

        -- Lọc ngoại lai Đường vào (hẻm không quá 50m)
        and (lane_width_m is null or lane_width_m between 0.5 and 50.0)

        -- Lọc ngoại lai Số phòng
        and (bedrooms is null or bedrooms <= 30)
        and (bathrooms is null or bathrooms <= 30)

)

select * from filtered_and_transformed

