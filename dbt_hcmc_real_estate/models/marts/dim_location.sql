{{
  config(
    materialized='table'
  )
}}

/*
===============================================================================
Model: dim_location
Layer: Marts (Star Schema - Dimension Table)
Description:
    Bảng thứ nguyên vị trí địa lý của bất động sản TP.HCM.
    Chứa danh sách duy nhất các địa điểm phân cấp theo:
    - district: Quận / Huyện / TP. Thủ Đức
    - ward: Phường / Xã / Thị trấn
    - street: Tên đường / Phố
===============================================================================
*/

with source_data as (

    select * from {{ ref('int_listings_deduplicated') }}

),

distinct_locations as (

    select distinct
        farm_fingerprint(
            concat(
                coalesce(district, 'UNKNOWN'), '|',
                coalesce(ward, 'UNKNOWN'), '|',
                coalesce(street, 'UNKNOWN')
            )
        )                                                               as location_key,
        district,
        ward,
        street
    from source_data

)

select * from distinct_locations

