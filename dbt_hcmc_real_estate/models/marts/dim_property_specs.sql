{{
  config(
    materialized='table'
  )
}}

/*
===============================================================================
Model: dim_property_specs
Layer: Marts (Star Schema - Dimension Table)
Description:
    Bảng thứ nguyên thuộc tính & pháp lý của bất động sản TP.HCM.
    Chứa danh sách duy nhất các bộ thuộc tính gồm:
    - lane_type: Loại đường vào (Mặt tiền, Hẻm ô tô, Hẻm xe máy, Chưa xác định)
    - legal_status: Tình trạng pháp lý (Sổ hồng, Sổ đỏ, Không có)
    - furniture_status: Tình trạng nội thất (Đầy đủ, Cơ bản, Trống / Nhà thô)
===============================================================================
*/

with source_data as (

    select * from {{ ref('int_listings_deduplicated') }}

),

distinct_specs as (

    select distinct
        farm_fingerprint(
            concat(
                coalesce(lane_type, 'UNKNOWN'), '|',
                coalesce(legal_status, 'UNKNOWN'), '|',
                coalesce(furniture_status, 'UNKNOWN')
            )
        )                                                               as property_spec_key,
        lane_type,
        legal_status,
        furniture_status
    from source_data

)

select * from distinct_specs

