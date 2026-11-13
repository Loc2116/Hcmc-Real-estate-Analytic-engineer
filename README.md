# 🏙️ HCMC Real Estate Modern Data Platform & Full-Stack Analytics (DE + DA)

[![HCMC Real Estate ELT Pipeline](https://github.com/Loc2116/Hcmc-Real-estate-Analytic-engineer/actions/workflows/elt_pipeline.yml/badge.svg)](https://github.com/Loc2116/Hcmc-Real-estate-Analytic-engineer/actions/workflows/elt_pipeline.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![dbt-bigquery 1.8+](https://img.shields.io/badge/dbt--bigquery-1.8+-orange.svg)](https://www.getdbt.com/)
[![Google BigQuery](https://img.shields.io/badge/Data%20Warehouse-Google%20BigQuery-4285F4.svg)](https://cloud.google.com/bigquery)
[![Backblaze B2](https://img.shields.io/badge/Data%20Lake-Backblaze%20B2-red.svg)](https://www.backblaze.com/b2)
[![Data Quality Tests](https://img.shields.io/badge/dbt%20tests-44%20passed%20(100%25)-brightgreen.svg)](https://github.com/Loc2116/Hcmc-Real-estate-Analytic-engineer)

Nền tảng dữ liệu hiện đại (Modern Data Platform) thu thập, lưu trữ, nạp idempotent, mô hình hóa dữ liệu theo kiến trúc **Medallion & Kimball Star Schema**, kiểm thử chất lượng dữ liệu tự động và lập lịch điều phối toàn trình (CI/CD Orchestration) phục vụ phân tích chuyên sâu thị trường Bất động sản TP.HCM.

---

## 📌 MỤC LỤC
1. [Kiến Trúc Tổng Thể Hệ Thống (ELT Architecture)](#1-kiến-trúc-tổng-thể-hệ-thống-elt-architecture)
2. [Tech Stack Chuẩn Công Nghiệp](#2-tech-stack-chuẩn-công-nghiệp)
3. [Mô Hình Hóa Dữ Liệu Star Schema (Kimball Modeling)](#3-mô-hình-hóa-dữ-liệu-star-schema-kimball-modeling)
4. [Tự Động Hóa & Điều Phối CI/CD (GitHub Actions)](#4-tự-động-hóa--điều-phối-cicd-github-actions)
5. [Hướng Dẫn Cấu Hình GitHub Secrets](#5-hướng-dẫn-cấu-hình-github-secrets)
6. [Cài Đặt & Chạy Thử Nghiệm Tại Local](#6-cài-đặt--chạy-thử-nghiệm-tại-local)
7. [Chất Lượng Dữ Liệu & Kiểm Thử Tự Động (Data Quality)](#7-chất-lượng-dữ-liệu--kiểm-thử-tự-động-data-quality)
8. [Tài Liệu Kèm Theo](#8-tài-liệu-kèm-theo)

---

## 1. KIẾN TRÚC TỔNG THỂ HỆ THỐNG (ELT ARCHITECTURE)

Hệ thống được thiết kế theo mô hình **ELT hiện đại (Extract - Load - Transform)**, chia tách độc lập giữa tầng lưu trữ Data Lake, kho dữ liệu phân tích Data Warehouse và tầng biến đổi mô hình hóa dữ liệu:

```mermaid
flowchart TD
    A["batdongsan.com.vn\n(scrapling_crawler.py)"] -->|Apache Parquet Snappy\nHive Partitioning| B["Data Lake: Backblaze B2\n(raw/year=YYYY/month=MM/day=DD/)"]
    B -->|load_bigquery.py\nWRITE_TRUNCATE| C["BigQuery Staging Table\n(raw_listings_staging)"]
    C -->|Idempotent Deduplication\nROW_NUMBER() OVER link| D["BigQuery Raw Table\n(raw_listings)"]
    D -->|dbt-bigquery\nstaging view| E["dbt Staging View\n(stg_listings)"]
    E -->|dbt-bigquery\nEntity Resolution| F["dbt Intermediate View\n(int_listings_deduplicated)"]
    F -->|dbt-bigquery\nKimball Star Schema| G["dbt Marts Star Schema\n- fact_listings (table)\n- dim_location (table)\n- dim_property_specs (table)\n- mart_listings_clean (OBT view)"]
    G -->|dbt test Quality Gate| H["Bộ 44 Kiểm thử tự động\n(Generic + Singular Tests: 100% PASS)"]
    H -->|Direct SQL Query & BI Connectors| I["Hệ thống BI & Analytics\n- Looker Studio Dashboard\n- Streamlit Market Explorer\n- Phân tích Bản đồ nhiệt & Thanh khoản"]
```

---

## 2. TECH STACK CHUẨN CÔNG NGHIỆP

| Tầng kiến trúc | Công nghệ áp dụng | Vai trò & Chi tiết triển khai |
|---|---|---|
| **Data Ingestion (DE)** | Python 3.11+, `requests`/`curl_cffi`, `scrapling`, `tenacity`, `pyarrow` | Cào dữ liệu tin đăng BĐS tự động, kháng chặn anti-bot (Cloudflare/WAF), retry exponential backoff, xuất Parquet Snappy phân vùng Hive-style. |
| **Data Lake (DE)** | **Backblaze B2** (S3-compatible API via `boto3`) | Lưu trữ dữ liệu thô dài hạn chuẩn Hive: `raw/year=YYYY/month=MM/day=DD/*.parquet`. Đồng bộ checkpoint `state/state.json` để chống trùng lặp trên runner stateless. |
| **Data Warehouse (DE)** | **Google BigQuery** (`google-cloud-bigquery`) | Lưu trữ kho dữ liệu phân tích. Nạp dữ liệu idempotent qua `load_bigquery.py` bằng staging table kết hợp `CREATE OR REPLACE TABLE` với `ROW_NUMBER() OVER (PARTITION BY link ORDER BY crawled_at DESC) = 1` (tương thích 100% với BigQuery Sandbox miễn phí). |
| **Data Modeling (DE/DA)** | **dbt-bigquery** (dbt Core) | Medallion Architecture: `staging` (view) $\rightarrow$ `intermediate` (view - Entity Resolution) $\rightarrow$ `marts` Star Schema (`fact_listings` dạng table, `dim_location`, `dim_property_specs`, `mart_listings_clean` OBT view). |
| **Data Quality (DE)** | **dbt test** (Generic & Singular Tests) | Kiểm thử tính toàn vẹn khóa ngoại (`relationships`), tính duy nhất (`unique`), không rỗng (`not_null`), miền giá trị (`accepted_values`), và điều kiện logic thương mại (giá/diện tích dương). |
| **Orchestration & CI/CD** | **GitHub Actions** | Tự động hóa toàn trình pipeline định kỳ hằng ngày lúc 02:00 AM VN (`cron: '0 19 * * *'` UTC) và hỗ trợ chạy thủ công (`workflow_dispatch`). |
| **BI & Analytics (DA)** | **Looker Studio / Streamlit / SQL** | Trực quan hóa bản đồ nhiệt giá, phân tích chênh lệch mặt tiền vs hẻm, đánh giá thanh khoản theo quận/huyện. |

---

## 3. MÔ HÌNH HÓA DỮ LIỆU STAR SCHEMA (KIMBALL MODELING)

Dữ liệu được chuẩn hóa và mô hình hóa theo cấu trúc **Kimball Star Schema**, 100% tên cột bằng tiếng Anh `snake_case` chuẩn công nghiệp:

```text
               ┌────────────────────────┐
               │      dim_location      │
               ├────────────────────────┤
               │ PK  location_key       │
               │     district           │
               │     ward               │
               │     street             │
               └───────────┬────────────┘
                           │ 1:N
                           │
┌──────────────────────────┴─────────────┐         ┌────────────────────────┐
│             fact_listings              │         │   dim_property_specs   │
├────────────────────────────────────────┤         ├────────────────────────┤
│ PK  listing_id                         │ N:1     │ PK  property_spec_key  │
│ FK  location_key                       ├─────────┤     lane_type          │
│ FK  property_spec_key                  │         │     legal_status       │
│     link                               │         │     furniture_status   │
│     crawled_at / crawled_date          │         └────────────────────────┘
│     price_billion_vnd                  │
│     area_m2                            │
│     price_per_m2                       │
│     lane_width_m                       │
│     frontage_m                         │
│     floors / bedrooms / bathrooms      │
│     duplicate_broker_count             │
│     is_lane_type_missing               │
│     is_lane_width_missing              │
└────────────────────────────────────────┘
```

1. **`fact_listings` (materialized: `table`):** Bảng sự kiện chứa các đại lượng đo lường (Measures: giá, diện tích, đơn giá, độ rộng đường, số tầng, số phòng, v.v.), chỉ số thanh khoản `duplicate_broker_count` và các cờ Missing Indicator.
2. **`dim_location` (materialized: `table`):** Bảng thứ nguyên vị trí địa lý (`location_key`, `district`, `ward`, `street`).
3. **`dim_property_specs` (materialized: `table`):** Bảng thứ nguyên quy cách bất động sản (`property_spec_key`, `lane_type`, `legal_status`, `furniture_status`).
4. **`mart_listings_clean` (materialized: `view`):** Unified One Big Table (OBT) kết hợp `fact_listings` `LEFT JOIN` với 2 bảng Dim, phục vụ truy vấn phân tích trực tiếp cho BI Dashboard và các nhà phân tích dữ liệu.

---

## 4. TỰ ĐỘNG HÓA & ĐIỀU PHỐI CI/CD (GITHUB ACTIONS)

Pipeline ELT được tự động hóa hoàn toàn bằng **GitHub Actions** tại file [`.github/workflows/elt_pipeline.yml`](.github/workflows/elt_pipeline.yml).

### 4.1. Cơ chế kích hoạt (Triggers)
- **Lập lịch tự động (Schedule):** Chạy mỗi ngày vào lúc **02:00 sáng giờ Việt Nam (UTC+7)**, tương ứng `cron: '0 19 * * *'` UTC.
- **Kích hoạt thủ công (Manual Workflow Dispatch):** Có thể chạy bất kỳ lúc nào qua giao diện GitHub Actions với tùy chọn điều chỉnh số lượng tin đăng cần cào (`crawler_samples`).

### 4.2. Luồng thực thi 9 bước tuần tự trong Runner
```text
1. Checkout mã nguồn (actions/checkout@v4)
2. Cài đặt Python 3.11 (actions/setup-python@v5 với pip cache)
3. Cài đặt thư viện (pip install -r requirements.txt)
4. Cấu hình GCP Service Account Credentials (tạo gcp_key.json từ secret GCP_SA_KEY)
5. Bước 1 - Data Ingestion: Cào tin mới & đồng bộ checkpoint B2 (python scrapling_crawler.py --upload-b2)
6. Bước 2 - Data Lake to DWH: Nạp idempotent từ B2 vào BigQuery (python load_bigquery.py)
7. Bước 3 - Transformations: Chạy dbt models (python -m dbt.cli.main run ...)
8. Bước 4 - Data Quality Gate: Chạy dbt tests (python -m dbt.cli.main test ...)
9. Bước 5 - Bảo mật: Xóa an toàn file gcp_key.json (luôn chạy if: always())
```

### 4.3. Cơ chế đồng bộ State Checkpoint chống trùng lặp trên môi trường Stateless
Vì GitHub Actions runner là môi trường tạm thời (stateless), crawler tích hợp cơ chế tự động:
1. **Trước khi cào:** Tải file `state/state.json` từ Backblaze B2 về nạp vào bộ nhớ để biết những URL/Property ID đã cào trước đây $\rightarrow$ Ngăn chặn tuyệt đối việc cào lặp lại các bài đăng cũ.
2. **Sau khi cào:** Đẩy ngược file `state/state.json` mới nhất lên Backblaze B2 $\rightarrow$ Đảm bảo phiên chạy kế tiếp luôn có dữ liệu checkpoint mới nhất.

---

## 5. HƯỚNG DẪN CẤU HÌNH GITHUB SECRETS

Để pipeline GitHub Actions có thể chạy thành công, bạn cần khai báo các Secrets sau trong repository GitHub của bạn (**Settings** $\rightarrow$ **Secrets and variables** $\rightarrow$ **Actions** $\rightarrow$ **New repository secret**):

| Tên Secret | Bắt buộc | Mô tả | Giá trị mẫu / Hướng dẫn |
|---|:---:|---|---|
| `B2_KEY_ID` | **Có** | Backblaze Application Key ID | Ví dụ: `005017f7fd53c670000000001` |
| `B2_APPLICATION_KEY` | **Có** | Backblaze Application Key bí mật | Ví dụ: `K005Htvuq2zCE0oDQYgZxlB0TYl2TTM` |
| `B2_BUCKET_NAME` | **Có** | Tên Bucket Data Lake trên B2 | `hcmc-real-estate-price-classification` |
| `B2_ENDPOINT_URL` | Không | S3 Endpoint của Backblaze B2 | `s3.us-east-005.backblazeb2.com` (mặc định đã có) |
| `GCP_PROJECT_ID` | **Có** | ID dự án Google Cloud | `hcmcrealestatepriceclf` |
| `BIGQUERY_DATASET` | Không | Tên Dataset BigQuery | `hcmc_real_estate` (mặc định đã có) |
| `BIGQUERY_LOCATION` | Không | Vùng lưu trữ BigQuery Dataset | `asia-southeast1` (mặc định đã có) |
| `GCP_SA_KEY` | **Có** | Toàn bộ nội dung JSON file Service Account Key GCP | Mở file `bigquery_key.json` cục bộ, copy toàn bộ nội dung `{ ... }` và paste vào secret này. Workflow hỗ trợ cả JSON thô lẫn chuỗi Base64. |

### Cách kích hoạt chạy thủ công qua GitHub Actions:
1. Truy cập tab **Actions** trên GitHub repository.
2. Chọn workflow **HCMC Real Estate ELT Pipeline** ở danh sách bên trái.
3. Nhấp vào nút **Run workflow**.
4. Nhập số lượng mẫu tin muốn cào (`crawler_samples`, mặc định là `50`) và bấm **Run workflow**.

---

## 6. CÀI ĐẶT & CHẠY THỬ NGHIỆM TẠI LOCAL

### Bước 1 — Chuẩn bị môi trường Python (3.11+)
```bash
# Tạo môi trường ảo
python -m venv .venv

# Kích hoạt trên Windows PowerShell:
.venv\Scripts\activate
# Hoặc trên Linux/macOS:
source .venv/bin/activate

# Cài đặt thư viện phụ thuộc
pip install --upgrade pip
pip install -r requirements.txt
```

### Bước 2 — Cấu hình file `.env`
Tạo file `.env` tại thư mục gốc của dự án:
```env
# BACKBLAZE B2 DATA LAKE
B2_ENDPOINT_URL=s3.us-east-005.backblazeb2.com
B2_KEY_ID=005017f7fd53c670000000001
B2_APPLICATION_KEY=your_b2_application_key_here
B2_BUCKET_NAME=hcmc-real-estate-price-classification

# GOOGLE BIGQUERY DATA WAREHOUSE
GCP_PROJECT_ID=hcmcrealestatepriceclf
BIGQUERY_DATASET=hcmc_real_estate
BIGQUERY_LOCATION=asia-southeast1
GOOGLE_APPLICATION_CREDENTIALS=bigquery_key.json
```

### Bước 3 — Chạy cào dữ liệu và đồng bộ Data Lake (B2)
```bash
# Cào 30 tin mới, tự động lưu Parquet Snappy phân vùng Hive và đẩy lên B2
python scrapling_crawler.py --samples 30 --upload-b2
```

### Bước 4 — Nạp dữ liệu Idempotent vào Google BigQuery
```bash
python load_bigquery.py
```

### Bước 5 — Chạy dbt để biến đổi mô hình Star Schema và kiểm thử
```bash
# Chạy chuyển đổi dữ liệu (staging -> intermediate -> marts)
python -m dbt.cli.main run --project-dir dbt_hcmc_real_estate --profiles-dir dbt_hcmc_real_estate

# Chạy kiểm thử chất lượng dữ liệu tự động
python -m dbt.cli.main test --project-dir dbt_hcmc_real_estate --profiles-dir dbt_hcmc_real_estate
```

---

## 7. CHẤT LƯỢNG DỮ LIỆU & KIỂM THỬ TỰ ĐỘNG (DATA QUALITY)

Dự án áp dụng bộ kiểm thử chất lượng dữ liệu tự động gồm **44 tests (100% PASS)** được định nghĩa trong `dbt_hcmc_real_estate/models/` và `dbt_hcmc_real_estate/tests/`:

1. **Kiểm thử khóa chính & tính duy nhất (`unique`):** Đảm bảo không trùng lặp bản ghi trên `listing_id`, `location_key`, `property_spec_key`.
2. **Kiểm thử trường bắt buộc (`not_null`):** Đảm bảo các thuộc tính định danh, giá, diện tích, đơn giá, thời gian không bị rỗng.
3. **Kiểm thử toàn vẹn tham chiếu (`relationships`):** Đảm bảo mọi khóa ngoại `location_key` và `property_spec_key` trong `fact_listings` đều tồn tại hợp lệ trong các bảng thứ nguyên tương ứng (`dim_location`, `dim_property_specs`).
4. **Kiểm thử miền giá trị hợp lệ (`accepted_values`):** Ràng buộc kiểu đường vào (`'Mặt tiền'`, `'Hẻm ô tô'`, `'Hẻm xe máy'`, `'Chưa xác định'`), pháp lý và tình trạng nội thất.
5. **Kiểm thử quy tắc nghiệp vụ (Singular SQL Tests):**
   - `assert_positive_price_and_area`: Kiểm tra giá niêm yết, diện tích và đơn giá luôn dương ($> 0$).
   - `assert_valid_hcmc_price_range`: Kiểm soát ngoại lai phi thực tế trên thị trường BĐS TP.HCM (diện tích $\in [10, 2000]\text{ m}^2$, đơn giá $\in [10, 1000]\text{ triệu VNĐ/m}^2$).

---

## 8. TÀI LIỆU KÈM THEO

- [CODEBOOK.md](CODEBOOK.md): Data Dictionary & từ điển đặc trưng chi tiết 100% tiếng Anh chuẩn công nghiệp.
- [GEMINI.md](GEMINI.md): Toàn bộ ngữ cảnh kiến trúc hệ thống, cấu hình B2, BigQuery và định hướng DA/DE.
- [dbt_hcmc_real_estate/](dbt_hcmc_real_estate/): Mã nguồn dự án dbt Core với mô hình hóa Kimball Star Schema.
- [Module/](Module/): Các notebook nghiên cứu và module cào dữ liệu gốc.

---
*Dự án cá nhân chuyên nghiệp của một Full-Stack Analytics Professional (Data Engineer & Analytics Engineer).*
