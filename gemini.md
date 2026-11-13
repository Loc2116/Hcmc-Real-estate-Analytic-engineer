# GEMINI CONTEXT — HCMC Real Estate Modern Data Platform & Full-Stack Analytics (DE + DA)
> **File lưu ngữ cảnh dự án (Context File)** phục vụ cho các mô hình AI/LLM và nhà phát triển để hiểu nhanh toàn bộ bài toán, kiến trúc hệ thống ELT, cấu hình Data Lake, Data Warehouse, mô hình hóa dữ liệu Star Schema (dbt) và lớp phân tích thị trường (Business Intelligence & Analytics).
> 
> *Cập nhật:* Tháng 11/2026 · *Phiên bản:* 4.0 (Full-Stack Data Engineering & Analytics Engineering)  
> *Môi trường:* Python 3.11+ / 3.14 · *Dataset:* Bất động sản TP.HCM từ `batdongsan.com.vn`

---

## 1. VAI TRÒ & MỤC TIÊU DỰ ÁN (ROLE & PROJECT GOAL)

- **Vai trò:** Full-Stack Analytics Professional (Senior Data Engineer & Analytics Engineer / BI Developer).
- **Định hướng dự án cá nhân:** Xây dựng nền tảng dữ liệu toàn diện (Full-Stack Modern Data Platform) tập trung chuyên sâu vào **Data Engineering (DE)** và **Data Analytics (DA)**, loại bỏ hoàn toàn các bài toán học máy gượng ép (DS/ML) để tối đa hóa tính thực chiến, độ tin cậy của dữ liệu và giá trị phân tích kinh doanh (Business Insights):
  1. **Data Ingestion (DE):** Cào dữ liệu tin đăng BĐS tự động, chống block, kháng lỗi với retry tự động (`tenacity`), xuất Parquet nén Snappy.
  2. **Data Lake (DE):** Lưu trữ dữ liệu thô chuẩn Hive-partitioning (`raw/year=YYYY/month=MM/day=DD/`) trên **Backblaze B2** (S3-compatible API).
  3. **Data Warehouse (DE):** Nạp dữ liệu idempotent vào **Google BigQuery** (staging table + deduplication theo link, tối ưu 100% cho BigQuery Sandbox).
  4. **Data Modeling / Analytics Engineering (DE/DA):** Chuẩn hóa và mô hình hóa dữ liệu nhiều tầng theo **Medallion Architecture & Kimball Star Schema** bằng **dbt-bigquery** (`stg_listings` -> `fact_listings`, `dim_location`, `dim_attributes`, `dim_date`).
  5. **Data Quality & Testing (DE):** Thiết lập bộ kiểm thử toàn vẹn dữ liệu tự động (`unique`, `not_null`, `accepted_values`) bằng dbt test.
  6. **Business Intelligence & Market Analytics (DA):** Xây dựng hệ thống báo cáo, trực quan hóa và dashboard phân tích chuyên sâu thị trường BĐS TP.HCM (Looker Studio / Streamlit / Power BI): bản đồ nhiệt giá m², biên độ chênh lệch mặt tiền vs hẻm, thanh khoản và tương quan diện tích - giá.
  7. **CI/CD & Orchestration (DE):** Tự động hóa pipeline định kỳ thông qua **GitHub Actions**.

---

## 2. TECH STACK ĐƯỢC PHÊ DUYỆT (STRICT APPROVED TECH STACK)

| Tầng kiến trúc | Công nghệ phê duyệt | Chi tiết cấu hình & Quy ước |
|---|---|---|
| **Extraction (Local)** | Python 3.11+, `requests`/`curl_cffi`, `tenacity`, `pandas`, `pyarrow` | Crawl dữ liệu sạch, tự động retry khi gặp lỗi mạng, xuất định dạng Parquet nén Snappy. |
| **Data Lake** | **Backblaze B2** (chuẩn S3 API qua `boto3`) | Không yêu cầu thẻ Visa. Phân vùng Hive-style: `raw/year=YYYY/month=MM/day=DD/listings_<timestamp>.parquet`. |
| **Data Warehouse** | **Google BigQuery** (`google-cloud-bigquery`) | Nạp dữ liệu idempotent qua `load_bigquery.py` (staging table `raw_listings_staging` kết hợp `CREATE OR REPLACE TABLE` với `ROW_NUMBER() OVER` theo `link`, tương thích BigQuery Sandbox miễn phí). |
| **Transformation / Modeling** | **dbt-bigquery** | Medallion Architecture: `staging` (view) -> `marts` Star Schema (`fact_listings` dạng table, `dim_location`, v.v.). |
| **Data Quality & CI/CD** | **dbt test**, **GitHub Actions** | Kiểm thử chất lượng dữ liệu tự động, lập lịch Cron định kỳ nạp BigQuery và chạy `dbt run/test`. |
| **Business Intelligence (DA)** | **Looker Studio / Streamlit / Metabase** | Xây dựng dashboard phân tích thị trường BĐS trực quan, kết nối trực tiếp tầng Marts BigQuery. |

---

## 3. CẤU HÌNH DATA LAKE: BACKBLAZE B2 (`storage_b2.py`)

Dự án sử dụng **Backblaze B2** qua giao thức S3-compatible API với thư viện `boto3`.

### 3.1. Các biến môi trường (.env)
File `.env` nằm tại thư mục gốc của dự án (đã được cấu hình trong `.gitignore`):
```env
# BACKBLAZE B2 (DATA LAKE)
B2_ENDPOINT_URL=s3.us-east-005.backblazeb2.com
B2_KEY_ID=your_b2_key_id_here
B2_APPLICATION_KEY=your_b2_application_key_here
B2_BUCKET_NAME=your_b2_bucket_name_here

# GOOGLE BIGQUERY (DATA WAREHOUSE)
GCP_PROJECT_ID=your_gcp_project_id_here
BIGQUERY_DATASET=hcmc_real_estate
GOOGLE_APPLICATION_CREDENTIALS=bigquery_key.json
```

### 3.2. Cấu trúc phân vùng Hive-style trên B2
```text
s3://hcmc-real-estate-price-classification/
└── raw/
    └── year=2026/
        └── month=05/
            └── day=12/
                ├── hcmc_listings_20260512_080000.parquet
                └── hcmc_listings_20260512_143000.parquet
```

### 3.3. Module `storage_b2.py`
Module cung cấp lớp `B2StorageManager` với các tính năng:
- **Xác thực chữ ký S3v4** với Backblaze B2, tự động chuẩn hóa URL (thêm `https://` nếu thiếu scheme).
- **Kháng lỗi mạng & Retry:** Tích hợp `tenacity` retry tự động đối với các lỗi tạm thời (`ClientError`, `ConnectTimeoutError`, `EndpointConnectionError`).
- **Xử lý In-Memory Streaming:** Ghi trực tiếp `pd.DataFrame` thành buffer Parquet (pyarrow snappy) và upload lên B2 mà không cần tạo file trung gian trên ổ cứng.
- **Tự động sinh Hive-Partition Key:** Hàm `build_partitioned_key(filename, dt)` tự động sinh đường dẫn `raw/year=YYYY/month=MM/day=DD/<filename>`.
- **Đọc trực tiếp:** Tải trực tiếp Parquet từ B2 vào `pd.DataFrame`.

---

## 4. CẤU TRÚC THƯ MỤC DỰ ÁN (PROJECT DIRECTORY TREE)

Dự án đã được chuẩn hóa về **1 cấp thư mục phẳng duy nhất**, loại bỏ hoàn toàn tình trạng thư mục con lồng nhau:

```text
hcmc-real-estate-price-classification-main/
│
├── .env                                 # Biến môi trường thực tế (B2 & BigQuery credentials)
├── .gitignore                           # Đã bảo vệ .env, bigquery_key.json, data_0/, data_1/
├── .python-version                      # Python 3.14 (hỗ trợ >= 3.11)
├── bigquery_key.json                    # GCP Service Account Key cho Google BigQuery
├── CODEBOOK.md                          # Data dictionary & chi tiết đặc trưng (v2.0)
├── README.md                            # Hướng dẫn chạy pipeline từ A đến Z
├── requirements.txt                     # Thư viện phụ thuộc chuẩn hóa (boto3, tenacity, pyarrow, etc.)
├── gemini.md                            # File ngữ cảnh chính này (Context file cho AI Agents)
├── load_bigquery.py                     # Script nạp dữ liệu Idempotent từ B2 Data Lake vào Google BigQuery (Sandbox compatible)
├── storage_b2.py                        # Module quản lý Backblaze B2 Data Lake (S3 API via boto3)
│
├── dbt_hcmc_real_estate/                # DỰ ÁN DBT CORE (dbt-bigquery, Medallion Architecture)
│   ├── dbt_project.yml                  # Cấu hình dbt (staging=view, marts=table cho Sandbox)
│   ├── profiles.yml                     # Profile BigQuery đọc từ env_var (không hardcode key)
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml              # Nguồn dữ liệu raw_listings
│   │   │   ├── schema.yml               # Test unique, not_null cho stg_listings
│   │   │   └── stg_listings.sql         # Model Staging chuẩn hóa Regex & kiểu dữ liệu (view)
│   │   ├── intermediate/
│   │   │   ├── schema.yml               # Test unique, not_null cho int_listings_deduplicated
│   │   │   └── int_listings_deduplicated.sql # Entity Resolution & khử trùng chéo môi giới (view)
│   │   └── marts/
│   │       ├── schema.yml               # Generic Tests (unique, not_null, relationships, accepted_values)
│   │       ├── dim_location.sql         # Bảng thứ nguyên Vị trí địa lý (table)
│   │       ├── dim_property_specs.sql   # Bảng thứ nguyên Thuộc tính & Pháp lý (table)
│   │       ├── fact_listings.sql        # Bảng sự kiện chính lưu metrics & measures (table)
│   │       └── mart_listings_clean.sql  # Unified Flat View kết hợp Star Schema (One Big Table view)
│   ├── analyses/
│   ├── macros/
│   ├── seeds/
│   ├── snapshots/
│   └── tests/
│       ├── assert_positive_price_and_area.sql # Singular test: Kiểm tra giá, diện tích dương (>0)
│       └── assert_valid_hcmc_price_range.sql  # Singular test: Lọc bỏ outliers giá và diện tích
│
├── data_0/                              # BỘ DỮ LIỆU BAN ĐẦU (4.719 tin cào gốc + pipeline ML)
│   ├── Raw/
│   │   ├── data_bds.csv                 # Dữ liệu cào thô ban đầu (~4.719 dòng, 15 cột)
│   │   └── data_bds _NaNFill.csv        # Dữ liệu đã điền sơ bộ từ mô tả
│   └── Processed/
│       ├── Cleaned_data/                # Output sau Notebook 02
│       │   ├── Cleaned_data.csv         # Dữ liệu gộp có nhãn Loại_Dữ_Liệu (Train/Test)
│       │   ├── X_train_cleaned.csv      # Features train sau làm sạch & imputation
│       │   ├── X_test_cleaned.csv       # Features test sau làm sạch & imputation
│       │   ├── y_train_cleaned.csv      # Target train sau làm sạch
│       │   └── y_test_cleaned.csv       # Target test sau làm sạch
│       ├── Cleaned_LLM/                 # Output trích xuất bằng Qwen 2.5 7B
│       │   └── Cleaned_data_qwen.csv    # Thêm 5 cột tiện ích POI (hem_xe_hoi, gan_cho_..., v.v.)
│       ├── Processed_data/              # Output sau Notebook 03 (Feature Engineering)
│       │   ├── X_train_processed.csv    # Features train đã mã hóa OHE, Ordinal, Target Encoding
│       │   ├── X_test_processed.csv     # Features test đã mã hóa
│       │   ├── y_train_processed.csv    # Target train
│       │   └── y_test_processed.csv     # Target test
│       └── ModelReady/                  # Artifacts mô hình phân loại (Notebook 04)
│           ├── best_pipeline_XGBoost.pkl# Pipeline mô hình tốt nhất (Preprocess + XGBoost)
│           ├── cv_results.csv           # Bảng so sánh 5-Fold Cross-Validation của 5 models
│           ├── X_train_model.csv        # 18 đặc trưng số đưa vào mô hình
│           ├── X_test_model.csv         # 18 đặc trưng số tập test
│           ├── y_train_encoded.csv      # Nhãn train mã hóa 0-4
│           ├── y_test_encoded.csv       # Nhãn test mã hóa 0-4
│           ├── class_distribution.png   # Biểu đồ phân phối 5 lớp giữa Train & Test
│           ├── cv_macro_f1_boxplot.png  # Boxplot Macro F1 qua 5 Folds
│           ├── confusion_matrix.png     # Ma trận nhầm lẫn của mô hình XGBoost
│           └── feature_importance.png   # Top 15 đặc trưng quan trọng nhất
│
├── data_1/                              # BỘ DỮ LIỆU CÀO MỚI (New Ingestion Source)
│   └── Raw/                             # Dữ liệu phân vùng Hive-style theo ngày cào (Apache Parquet Snappy)
│       ├── data_bds_scrapling_raw.csv   # Dữ liệu CSV cũ (Legacy ingestion source)
│       └── year=YYYY/
│           └── month=MM/
│               └── day=DD/
│                   └── listings_YYYYMMDD_HHMMSS.parquet
│
└── Module/                           # 4 Notebook nghiên cứu & thử nghiệm ML gốc
    ├── 01_data_collection/
    │   ├── 01_Scrapling_Crawler_Pipeline.ipynb # Selenium cào dữ liệu từ batdongsan.com.vn
    │   └── hrefs.txt                    # Danh sách URL bài đăng đã cào
    ├── 02_data_cleaning_and_imputation/
    │   └── 02_data_cleaning_classification.ipynb # Làm sạch, tách địa chỉ, KNN & MICE Imputation
    ├── 03_exploratory_data_analysis/
    │   ├── 03_EDA_FeatureEngineering.ipynb       # EDA, OHE, Ordinal, Smoothed Target Encoding
    │   ├── llm-qwen-extract-bds.ipynb            # Trích xuất POI qua Qwen 2.5 7B (Kaggle T4)
    │   ├── eda_raw.png                           # Trực quan hóa dữ liệu thô
    │   └── eda_after_preprocessing_outputs/      # Biểu đồ phân tích chuyên sâu
    │       └── figures/
    │           ├── 01_listing_count_by_Quận.png
    │           ├── 02_median_unit_price_by_Quận.png
    │           └── 03_area_group_unit_price_premium.png
    └── 04_modeling/
        └── 04_ClassifyModeling.ipynb             # KMeans tạo nhãn, CV 5-fold, huấn luyện XGBoost
```

---

## 5. TỔNG QUAN LUỒNG DỮ LIỆU FULL-STACK ANALYTICS (ELT & BI)

```mermaid
flowchart TD
    A["batdongsan.com.vn (scrapling_crawler.py)"] -->|Parquet Snappy & Hive Partitioning| B["data_1/Raw/year=.../month=.../day=.../*.parquet"]
    B -->|storage_b2.py (boto3 S3 API)| C["Backblaze B2 Data Lake (Hive Parquet: raw/year=.../)"]
    C -->|load_bigquery.py (WRITE_TRUNCATE)| D["BigQuery Staging Table (raw_listings_staging)"]
    D -->|Idempotent Deduplication by link| E["BigQuery Raw Table (raw_listings)"]
    E -->|dbt-bigquery (staging)| F["dbt Staging View (stg_listings)"]
    F -->|dbt-bigquery (intermediate)| G["dbt Intermediate View (int_listings_deduplicated)"]
    G -->|dbt-bigquery (marts)| H["dbt Marts: Star Schema\n- fact_listings (table)\n- dim_location (table)\n- dim_property_specs (table)\n- mart_listings_clean (OBT view)"]
    H -->|dbt test & Quality Gate| I["Bộ kiểm thử tự động (37 tests: Generic + Singular) PASS 100%"]
    I -->|Direct SQL Query & BI Connectors| K["Hệ thống Phân tích & Trực quan hóa (BI & Analytics)\n- Looker Studio / Streamlit Dashboard\n- Bản đồ nhiệt giá BĐS TP.HCM\n- Phân tích thanh khoản & chênh lệch Mặt tiền vs Hẻm"]
```

---

## 6. MÔ HÌNH HÓA DỮ LIỆU STAR SCHEMA & CHỈ SỐ PHÂN TÍCH THỊ TRƯỜNG

### 6.1. Thiết kế Star Schema (Kimball Dimensional Modeling)
Dự án mô hình hóa dữ liệu theo cấu trúc **Kimball Star Schema** tối ưu cho truy vấn phân tích (OLAP/BI):

1. **Bảng Sự kiện — `fact_listings` (materialized: `table`):**
   - **Khóa chính:** `listing_id` (PK, phái sinh từ `property_fingerprint`).
   - **Khóa ngoại:** `location_key` (FK trỏ tới `dim_location`), `property_spec_key` (FK trỏ tới `dim_property_specs`).
   - **Định danh & Thời gian:** `link`, `crawled_at`, `crawled_date`.
   - **Các chỉ số đo lường (Measures/Metrics):**
     - `price_billion_vnd`: Giá niêm yết (tỷ VNĐ).
     - `area_m2`: Diện tích đất ($\text{m}^2$).
     - `price_per_m2`: Đơn giá thị trường ($\text{triệu VNĐ/m}^2$).
     - `lane_width_m`: Độ rộng đường trước nhà (m).
     - `frontage_m`: Chiều ngang mặt tiền (m).
     - `floors`: Số tầng nhà.
     - `bedrooms`, `bathrooms`, `total_rooms`: Thông số phòng.
     - `duplicate_broker_count`: Số môi giới cùng rao bán (chỉ số thanh khoản).
     - `is_lane_type_missing`, `is_lane_width_missing`: Missing indicators (MNAR).

2. **Bảng Thứ nguyên Vị trí — `dim_location` (materialized: `table`):**
   - **Khóa chính:** `location_key` (Surrogate Key hash từ District + Ward + Street).
   - **Thuộc tính:** `district` (Quận/Huyện), `ward` (Phường/Xã), `street` (Đường phố).

3. **Bảng Thứ nguyên Thuộc tính — `dim_property_specs` (materialized: `table`):**
   - **Khóa chính:** `property_spec_key` (Surrogate Key hash từ Lane Type + Legal + Furniture).
   - **Thuộc tính:** `lane_type` (Mặt tiền, Hẻm ô tô, Hẻm xe máy, Chưa xác định), `legal_status` (Sổ hồng, Sổ đỏ, Không có), `furniture_status` (Đầy đủ, Cơ bản, Trống / Nhà thô).

4. **Bảng Phẳng Hợp nhất — `mart_listings_clean` (materialized: `view`):**
   - View One Big Table (OBT) kết hợp `fact_listings` `LEFT JOIN` với `dim_location` và `dim_property_specs`, phục vụ truy vấn phân tích trực tiếp trong Notebook và BI.

### 6.2. Các câu hỏi nghiệp vụ và chỉ số phân tích trọng tâm (Core DA Metrics)
1. **Bản đồ nhiệt & Đơn giá theo Quận/Huyện:**
   - Trung vị đơn giá (`median(price_per_m2)`) và phân vị $25\% - 75\%$ tại 24 Quận/Huyện TP.HCM.
   - Tìm kiếm các vùng trũng giá (Undervalued Areas) so với khoảng cách đến trung tâm (Quận 1).
2. **Chênh lệch giá trị Mặt tiền vs Hẻm (Alley vs Frontage Premium):**
   - Tỷ lệ chênh lệch đơn giá ($\%$) giữa `Mặt tiền`, `Hẻm ô tô` ($\ge 4\text{m}$) và `Hẻm nhỏ`.
3. **Mối tương quan giữa Độ rộng đường & Đơn giá (Price Elasticity vs Lane Width):**
   - Đánh giá độ nhạy của giá nhà khi độ rộng đường vào tăng thêm mỗi $1\text{m}$.
4. **Mật độ tin đăng & Xu hướng thanh khoản:**
   - Số lượng tin đăng phân bổ theo khu vực và phân khúc diện tích ($<30\text{m}^2$, $30-60\text{m}^2$, $60-100\text{m}^2$, $>100\text{m}^2$).

---

## 7. QUY ƯỚC PHÁT TRIỂN & CÁC BƯỚC TIẾP THEO

- **Tương tác Data Lake:** Mọi thao tác tải lên/tải xuống Backblaze B2 phải thông qua [storage_b2.py](storage_b2.py).
- **Tính Idempotent BigQuery:** Nạp dữ liệu vào Google BigQuery thông qua [load_bigquery.py](load_bigquery.py). Sử dụng bảng staging `raw_listings_staging` kết hợp `CREATE OR REPLACE TABLE` với `ROW_NUMBER() OVER (PARTITION BY link ORDER BY crawled_at DESC) = 1` để tránh duplicate bản ghi và chạy an toàn trên BigQuery Sandbox miễn phí.
- **Dữ liệu mới (`data_1/Raw/`):** Thu thập trực tiếp dưới định dạng Apache Parquet (Snappy) phân vùng Hive-style `year=YYYY/month=MM/day=DD/listings_<timestamp>.parquet`.
- **Chạy nạp BigQuery:**
  ```bash
  # Nạp toàn bộ dữ liệu từ B2 vào BigQuery
  python load_bigquery.py
  ```
- **Chạy biến đổi dữ liệu với dbt-bigquery:**
  ```bash
  # Chạy biến đổi toàn bộ models (staging -> marts Star Schema)
  python -m dbt.cli.main run --project-dir dbt_hcmc_real_estate --profiles-dir dbt_hcmc_real_estate

  # Chạy kiểm thử chất lượng dữ liệu (tests)
  python -m dbt.cli.main test --project-dir dbt_hcmc_real_estate --profiles-dir dbt_hcmc_real_estate
  ```
- **Tự động hóa toàn trình (CI/CD Orchestration):** Workflow GitHub Actions tại `.github/workflows/elt_pipeline.yml` tự động chạy hằng ngày lúc 02:00 AM VN (`cron: '0 19 * * *'` UTC) và hỗ trợ chạy thủ công (`workflow_dispatch`). Checkpoint `state/state.json` được đồng bộ 2 chiều với Backblaze B2 để chống trùng lặp dữ liệu trên runner stateless.
- **Các bước tiếp theo (Lộ trình DA + BI chuyên sâu):**
  1. **Xây dựng Dashboard phân tích thị trường BĐS (Business Intelligence):**
     - Lựa chọn 1: Dựng Web App tương tác bằng **Streamlit** (bản đồ trực quan Plotly/Folium, bộ lọc Quận/Giá, biểu đồ phân tích tương tác trực tiếp với BigQuery hoặc Marts data).
     - Lựa chọn 2: Kết nối **Looker Studio** trực tiếp với Google BigQuery (`mart_listings_clean` hoặc Star Schema) để tạo báo cáo Executive BI chia sẻ công khai.
  2. **Viết Báo cáo Portfolio & Case Study (Portfolio Documentation):**
     - Soạn thảo tài liệu Case Study chuẩn Portfolio cá nhân làm nổi bật các bài toán kỹ thuật: Entity Resolution chống trùng lặp môi giới, Hive Parquet B2 Data Lake, BigQuery Sandbox idempotent loader, Star Schema Kimball Modeling và Data Quality Testing.
