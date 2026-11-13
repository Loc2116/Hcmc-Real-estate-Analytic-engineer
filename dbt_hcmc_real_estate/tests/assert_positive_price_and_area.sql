-- ===============================================================================
-- Singular Test: assert_positive_price_and_area
-- Description:
--     Kiểm tra tính hợp lệ cơ bản của dữ liệu kinh tế:
--     Giá niêm yết (tỷ VNĐ), Diện tích (m²), và Đơn giá (triệu VNĐ/m²)
--     bắt buộc phải là số thực dương (> 0).
--     Bất kỳ bản ghi nào có giá trị <= 0 hoặc NULL sẽ trả về và gây lỗi test.
-- ===============================================================================

select
    listing_id,
    price_billion_vnd,
    area_m2,
    price_per_m2
from {{ ref('fact_listings') }}
where
    price_billion_vnd <= 0
    or area_m2 <= 0
    or price_per_m2 <= 0
    or price_billion_vnd is null
    or area_m2 is null
    or price_per_m2 is null

