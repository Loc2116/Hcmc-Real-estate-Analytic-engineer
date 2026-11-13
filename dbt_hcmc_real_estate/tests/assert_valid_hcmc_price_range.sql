-- ===============================================================================
-- Singular Test: assert_valid_hcmc_price_range
-- Description:
--     Kiểm tra các giá trị ngoại lai (outliers) không được phép lọt vào fact_listings:
--     - Đơn giá BĐS TP.HCM không vượt quá 2.5 tỷ VNĐ/m² (2500.0 triệu VNĐ/m²).
--     - Diện tích đất không vượt quá 2000.0 m².
--     Bất kỳ bản ghi nào vượt ngưỡng trên sẽ trả về và gây lỗi test.
-- ===============================================================================

select
    listing_id,
    area_m2,
    price_per_m2
from {{ ref('fact_listings') }}
where
    price_per_m2 > 2500.0
    or area_m2 > 2000.0

