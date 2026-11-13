{{
  config(
    materialized='view'
  )
}}

/*
===============================================================================
Model: int_listings_deduplicated
Layer: Intermediate (Medallion: Silver)
Description:
    Tầng Intermediate thực hiện khử trùng lặp dữ liệu và Entity Resolution:
    1. Khử trùng lặp theo URL/link gốc (ROW_NUMBER partition by link).
    2. Khử trùng lặp vật lý (Entity Resolution): gộp các bài đăng của nhiều môi
       giới cùng rao bán 1 bất động sản thực tế dựa trên property_fingerprint.
    3. Đếm số lượng tin đăng môi giới trùng lặp (duplicate_broker_count).
===============================================================================
*/

with staging as (

    select * from {{ ref('stg_listings') }}

),

-- ============================================================================
-- BƯỚC 1: KHỬ TRÙNG LẶP THEO URL / ĐƯỜNG DẪN GỐC (Link Deduplication)
-- Loại bỏ các bản ghi cào lặp lại cùng một link bài đăng, giữ lại bản ghi mới nhất.
-- ============================================================================
dedup_by_url as (

    select
        *,
        row_number() over (
            partition by link
            order by crawled_at desc
        ) as url_rn
    from staging

),

url_deduplicated as (

    select * except(url_rn)
    from dedup_by_url
    where url_rn = 1

),

-- ============================================================================
-- BƯỚC 2: KHỬ TRÙNG CHÉO TIN ĐĂNG CỦA NHIỀU MÔI GIỚI (Entity Resolution)
-- Nhiều môi giới cùng rao bán 1 căn nhà thực tế với thông số giống nhau:
-- - Cùng Quận, Phường, Tên Đường
-- - Cùng Diện tích (làm tròn 1 chữ số thập phân)
-- - Cùng Số tầng
-- - Cùng Giá bán (làm tròn 2 chữ số thập phân)
-- ============================================================================
with_property_fingerprint as (

    select
        *,

        /*
        -----------------------------------------------------------------------
        GHI CHÚ ĐIỀU CHỈNH LUẬT GỘP TRÙNG (ENTITY RESOLUTION RULES):
        - Khóa fingerprint kết hợp:
          1. listing_type: Phân loại loại hình (Căn hộ, Nhà riêng, Biệt thự...)
          2. district: Quận/Huyện
          3. ward: Phường/Xã
          4. street: Tên đường/phố
          5. area_m2: Diện tích làm tròn 1 chữ số
          6. floors: Số tầng (mặc định -1 nếu khuyết)
          7. bedrooms: Số phòng ngủ (mặc định -1 nếu khuyết, phân biệt 2PN vs 3PN)
          8. price_billion_vnd: Giá làm tròn 2 chữ số
        
        - Muốn luật CHẶT CHẼ HƠN (ít gộp trùng hơn, tránh gộp nhầm 2 căn cạnh nhau):
          Bổ sung thêm: `COALESCE(CAST(ROUND(frontage_m, 1) AS STRING), '')`,
                        `COALESCE(CAST(ROUND(lane_width_m, 1) AS STRING), '')`,
                        `COALESCE(house_direction, '')`.
        - Muốn luật NỚI LỎNG HƠN (gộp nhiều hơn kể cả khi môi giới kê chênh giá nhẹ):
          Bỏ bớt `price_billion_vnd` hoặc làm tròn giá đến hàng trăm triệu.
        -----------------------------------------------------------------------
        */
        farm_fingerprint(
            concat(
                coalesce(listing_type, ''), '|',
                coalesce(district, ''), '|',
                coalesce(ward, ''), '|',
                coalesce(street, ''), '|',
                coalesce(cast(round(area_m2, 1) as string), 'na_area'), '|',
                cast(coalesce(floors, -1) as string), '|',
                cast(coalesce(bedrooms, -1) as string), '|',
                coalesce(cast(round(price_billion_vnd, 2) as string), 'na_price')
            )
        ) as property_fingerprint

    from url_deduplicated

),

broker_deduplicated as (

    select
        *,
        -- Đếm số lượng tin đăng / môi giới cùng rao bán cùng 1 căn nhà thực tế
        count(*) over (
            partition by property_fingerprint
        ) as duplicate_broker_count,

        -- Giữ lại bản ghi đại diện có thông tin được cào mới nhất
        row_number() over (
            partition by property_fingerprint
            order by crawled_at desc
        ) as property_rn
    from with_property_fingerprint

),

final as (

    select * except(property_rn)
    from broker_deduplicated
    where property_rn = 1

)

select * from final

