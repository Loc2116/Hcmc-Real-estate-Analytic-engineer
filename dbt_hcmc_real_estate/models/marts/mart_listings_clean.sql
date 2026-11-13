{{
  config(
    materialized='table'
  )
}}

/*
===============================================================================
Model: mart_listings_clean
Layer: Marts (One Big Table / Unified Flat View)
Description:
    Bảng phẳng hợp nhất (One Big Table - OBT) kết hợp từ kiến trúc Star Schema:
    - Bảng Sự kiện: fact_listings
    - Bảng Thứ nguyên Vị trí: dim_location
    - Bảng Thứ nguyên Thuộc tính: dim_property_specs
    Phục vụ trực tiếp cho báo cáo, trực quan hóa BI (Looker Studio / Streamlit)
    và các bài phân tích dữ liệu dạng Notebook / SQL đơn giản.
===============================================================================
*/

with fact as (

    select * from {{ ref('fact_listings') }}

),

dim_loc as (

    select * from {{ ref('dim_location') }}

),

dim_specs as (

    select * from {{ ref('dim_property_specs') }}

),

joined as (

    select
        -- 1. Khóa định danh & Metadata
        fact.listing_id,
        fact.link,
        fact.link                                                       as listing_url,
        fact.crawled_at,
        fact.crawled_date,
        fact.location_key,
        fact.property_spec_key,

        -- 2. Địa giới hành chính (dim_location)
        dim_loc.district,
        dim_loc.ward,
        dim_loc.street,

        -- 3. Đặc trưng & Pháp lý bất động sản (dim_property_specs)
        dim_specs.lane_type,
        dim_specs.legal_status,
        dim_specs.furniture_status,

        -- 4. Giá & Diện tích cốt lõi (fact_listings)
        fact.price_billion_vnd,
        fact.area_m2,
        fact.price_per_m2,

        -- 5. Thông số kỹ thuật nhà đất
        fact.floors,
        fact.bedrooms,
        fact.bathrooms,
        fact.total_rooms,
        fact.lane_width_m,
        fact.frontage_m,

        -- 6. Chỉ số thanh khoản & Cờ thiếu dữ liệu (MNAR)
        fact.duplicate_broker_count,
        fact.is_lane_type_missing,
        fact.is_lane_width_missing

    from fact
    left join dim_loc
        on fact.location_key = dim_loc.location_key
    left join dim_specs
        on fact.property_spec_key = dim_specs.property_spec_key

)

select * from joined
