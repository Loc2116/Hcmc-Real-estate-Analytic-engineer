"""
Application: Dashboard/app.py
Description: Interactive & Auto-Refreshing Market Intelligence Dashboard for HCMC Real Estate
             powered by dbt Marts (mart_listings_clean) from Google BigQuery or Local Parquet.
Usage:
    python -m streamlit run Dashboard/app.py
"""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DASHBOARD_DIR = Path(__file__).resolve().parent
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

from dashboard_analytics import (
    AREA_SEGMENT_ORDER,
    LANE_TYPE_ORDER,
    compute_kpis,
    compute_lane_width_elasticity,
    load_mart_data,
    summarize_by_dimension,
)

# -----------------------------------------------------------------------------
# Page Configuration & Styling
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="HCMC Real Estate Market Intelligence",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }
        div[data-testid="stMetric"] {
            background-color: #F8FAFC;
            border: 1px solid #E2E8F0;
            border-radius: 10px;
            padding: 12px 16px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


def _read_gcp_secrets() -> tuple[Optional[dict], Optional[str], Optional[str]]:
    """Safely extract Streamlit Cloud secrets (TOML table or raw JSON string) without crashing locally."""
    try:
        secrets = st.secrets
        creds = None
        if "gcp_service_account" in secrets:
            creds = dict(secrets["gcp_service_account"])
        elif "GCP_SA_KEY" in secrets:
            import json

            creds = json.loads(secrets["GCP_SA_KEY"])
        project_id = secrets.get("GCP_PROJECT_ID")
        dataset_id = secrets.get("BIGQUERY_DATASET")
        return creds, project_id, dataset_id
    except Exception:
        return None, None, None


@st.cache_data(ttl=3600, show_spinner="Đang đồng bộ dữ liệu từ tầng Marts...")
def get_cached_mart_data(mode: str) -> tuple[pd.DataFrame, str, Optional[str]]:
    """Load mart_listings_clean with 1-hour TTL cache so daily pipeline updates appear automatically."""
    creds_info, project_id, dataset_id = _read_gcp_secrets()
    return load_mart_data(
        mode=mode,
        credentials_info=creds_info,
        project_id=project_id,
        dataset_id=dataset_id,
    )


def _format_num(val: Optional[float], suffix: str = "", decimals: int = 1) -> str:
    if val is None or pd.isna(val):
        return "N/A"
    return f"{val:,.{decimals}f}{suffix}"


# -----------------------------------------------------------------------------
# Sidebar Controls & Data Source Selector
# -----------------------------------------------------------------------------
with st.sidebar:
    st.header("⚙️ Nguồn dữ liệu & Bộ lọc")

    source_choice = st.radio(
        "Chế độ tải dữ liệu",
        options=["auto", "bigquery", "local"],
        format_func=lambda x: {
            "auto": "Tự động (BigQuery → Local)",
            "bigquery": "Google BigQuery (Live)",
            "local": "Local Snapshot (data_exports)",
        }[x],
        index=0,
    )

    if st.button("🔄 Làm mới dữ liệu ngay", width="stretch"):
        st.cache_data.clear()
        st.rerun()

try:
    df, source_label, fallback_warning = get_cached_mart_data(source_choice)
except Exception as exc:
    st.error(f"Không thể tải dữ liệu mart_listings_clean: {exc}")
    st.stop()

# -----------------------------------------------------------------------------
# Header
# -----------------------------------------------------------------------------
latest_date = df["crawled_date"].max() if "crawled_date" in df.columns and not df.empty else None
st.title("🏙️ HCMC Real Estate Market Intelligence")
st.caption(
    f"Nguồn: **{source_label}** · Cập nhật dữ liệu mới nhất: **{latest_date or 'N/A'}** · "
    "Dữ liệu đã khử trùng chéo môi giới (Entity Resolution) qua dbt Star Schema"
)
if fallback_warning:
    st.info(fallback_warning)

# -----------------------------------------------------------------------------
# Sidebar Filters
# -----------------------------------------------------------------------------
with st.sidebar:
    st.divider()

    district_counts = df["district_label"].value_counts()
    all_districts = [d for d in district_counts.index.tolist() if d != "Chưa xác định"]
    if "Chưa xác định" in district_counts.index:
        all_districts.append("Chưa xác định")

    selected_districts = st.multiselect(
        "Quận / Huyện",
        options=all_districts,
        default=[],
        placeholder="Tất cả Quận/Huyện",
    )

    available_lanes = [lt for lt in LANE_TYPE_ORDER if lt in df["lane_type"].unique()]
    selected_lanes = st.multiselect(
        "Loại đường vào",
        options=available_lanes,
        default=available_lanes,
    )

    available_legals = sorted(df["legal_status"].dropna().unique().tolist())
    selected_legals = st.multiselect(
        "Pháp lý",
        options=available_legals,
        default=available_legals,
    )

    available_segments = [s for s in AREA_SEGMENT_ORDER if s in df["area_segment"].dropna().unique()]
    selected_segments = st.multiselect(
        "Phân khúc diện tích",
        options=available_segments,
        default=available_segments,
    )

    p_min, p_max = float(df["price_billion_vnd"].min()), float(df["price_billion_vnd"].max())
    price_range = st.slider(
        "Khoảng giá niêm yết (Tỷ VNĐ)",
        min_value=round(p_min, 1),
        max_value=round(p_max, 1),
        value=(round(p_min, 1), round(p_max, 1)),
    )

    only_multi_broker = st.checkbox("Chỉ xem BĐS có ≥ 2 môi giới rao bán", value=False)

# Apply Filters
filtered_df = df.copy()
if selected_districts:
    filtered_df = filtered_df[filtered_df["district_label"].isin(selected_districts)]
if selected_lanes:
    filtered_df = filtered_df[filtered_df["lane_type"].isin(selected_lanes)]
if selected_legals:
    filtered_df = filtered_df[filtered_df["legal_status"].isin(selected_legals)]
if selected_segments:
    filtered_df = filtered_df[filtered_df["area_segment"].astype(str).isin(selected_segments)]

filtered_df = filtered_df[
    (filtered_df["price_billion_vnd"] >= price_range[0])
    & (filtered_df["price_billion_vnd"] <= price_range[1])
]
if only_multi_broker:
    filtered_df = filtered_df[filtered_df["duplicate_broker_count"] >= 2]

if filtered_df.empty:
    st.warning("Không có bất động sản nào khớp với bộ lọc hiện tại. Hãy nới lỏng điều kiện lọc ở thanh bên trái.")
    st.stop()

# -----------------------------------------------------------------------------
# KPI Cards
# -----------------------------------------------------------------------------
kpis = compute_kpis(filtered_df)
col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("BĐS Duy nhất (Physical)", f"{kpis['total_listings']:,}")
col2.metric("Trung vị Giá bán", _format_num(kpis["median_price_billion"], " Tỷ", 2))
col3.metric("Trung vị Đơn giá", _format_num(kpis["median_price_per_m2"], " Tr/m²", 1))
col4.metric(
    "Chênh lệch Mặt tiền vs Hẻm",
    _format_num(kpis["frontage_premium_pct"], "%", 1) if kpis["frontage_premium_pct"] is not None else "N/A",
    help="Tỷ lệ chênh lệch trung vị đơn giá/m² giữa nhà Mặt tiền và nhà Hẻm (ô tô + xe máy).",
)
col5.metric(
    "Tỷ lệ trùng chéo Môi giới",
    _format_num(kpis["multi_broker_share_pct"], "%", 1),
    delta=f"TB {kpis['avg_broker_count']:.2f} môi giới/căn",
    delta_color="off",
)

st.divider()

# -----------------------------------------------------------------------------
# Interactive Analytics Tabs
# -----------------------------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs(
    [
        "📍 Đơn giá & Vùng trũng Quận/Huyện",
        "🛣️ Mặt tiền vs Hẻm & Độ rộng đường",
        "📐 Phân khúc Diện tích, Pháp lý & Thanh khoản",
        "🔎 Tra cứu & Xuất dữ liệu",
    ]
)

# --- TAB 1: District Price Distribution & Supply ---
with tab1:
    min_n = st.slider("Số tin tối thiểu mỗi Quận để xếp hạng", min_value=1, max_value=30, value=5)
    dist_summary = summarize_by_dimension(filtered_df, "district_label", min_count=min_n)
    dist_summary = dist_summary[dist_summary["district_label"] != "Chưa xác định"].sort_values(
        "median_price_per_m2", ascending=True
    )

    if dist_summary.empty:
        st.info("Chưa đủ mẫu để thống kê theo Quận với ngưỡng lọc hiện tại.")
    else:
        c_left, c_right = st.columns([1.3, 1.0])

        with c_left:
            st.subheader("Trung vị & Khoảng phân vị (P25–P75) Đơn giá theo Quận")
            fig_iqr = go.Figure()
            for _, row in dist_summary.iterrows():
                fig_iqr.add_trace(
                    go.Scatter(
                        x=[row["p25_price_per_m2"], row["p75_price_per_m2"]],
                        y=[row["district_label"], row["district_label"]],
                        mode="lines",
                        line=dict(color="#94A3B8", width=6),
                        showlegend=False,
                        hoverinfo="skip",
                    )
                )
            fig_iqr.add_trace(
                go.Scatter(
                    x=dist_summary["median_price_per_m2"],
                    y=dist_summary["district_label"],
                    mode="markers+text",
                    marker=dict(color="#2563EB", size=10),
                    text=dist_summary["median_price_per_m2"].map(lambda v: f"{v:.0f}"),
                    textposition="middle right",
                    name="Trung vị (Tr/m²)",
                    customdata=dist_summary[["p25_price_per_m2", "p75_price_per_m2", "listing_count"]],
                    hovertemplate=(
                        "<b>%{y}</b><br>"
                        "Trung vị: %{x:.1f} Tr/m²<br>"
                        "P25–P75: %{customdata[0]:.1f} – %{customdata[1]:.1f} Tr/m²<br>"
                        "Số BĐS: %{customdata[2]:,}<extra></extra>"
                    ),
                )
            )
            fig_iqr.update_layout(
                height=max(420, len(dist_summary) * 26),
                margin=dict(l=10, r=40, t=20, b=30),
                xaxis_title="Đơn giá (Triệu VNĐ / m²)",
                yaxis_title="",
                showlegend=False,
            )
            st.plotly_chart(fig_iqr, width="stretch")

        with c_right:
            st.subheader("Mật độ nguồn cung theo Loại đường")
            top_districts = dist_summary["district_label"].tolist()
            supply_df = filtered_df[filtered_df["district_label"].isin(top_districts)]
            fig_supply = px.histogram(
                supply_df,
                y="district_label",
                color="lane_type",
                category_orders={"lane_type": LANE_TYPE_ORDER},
                barmode="stack",
                color_discrete_sequence=px.colors.qualitative.Set2,
                labels={"district_label": "", "count": "Số BĐS", "lane_type": "Loại đường"},
            )
            fig_supply.update_yaxes(categoryorder="array", categoryarray=top_districts)
            fig_supply.update_layout(
                height=max(420, len(dist_summary) * 26),
                margin=dict(l=10, r=10, t=20, b=30),
                xaxis_title="Số lượng BĐS duy nhất",
                legend=dict(orientation="h", yanchor="bottom", y=1.01, x=0),
            )
            st.plotly_chart(fig_supply, width="stretch")

# --- TAB 2: Frontage vs Alley & Lane Width Elasticity ---
with tab2:
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Phân phối đơn giá: Mặt tiền vs Hẻm")
        fig_box = px.box(
            filtered_df,
            x="lane_type",
            y="price_per_m2",
            color="lane_type",
            category_orders={"lane_type": LANE_TYPE_ORDER},
            points=False,
            color_discrete_sequence=px.colors.qualitative.Safe,
            labels={"lane_type": "Loại đường", "price_per_m2": "Đơn giá (Triệu VNĐ/m²)"},
        )
        fig_box.update_layout(height=420, showlegend=False, margin=dict(t=20, b=30))
        st.plotly_chart(fig_box, width="stretch")

        lane_summary = summarize_by_dimension(filtered_df, "lane_type")
        if not lane_summary.empty:
            st.dataframe(
                lane_summary[
                    ["lane_type", "listing_count", "share_pct", "median_price_per_m2", "median_price_billion"]
                ].rename(
                    columns={
                        "lane_type": "Loại đường",
                        "listing_count": "Số BĐS",
                        "share_pct": "Tỷ trọng (%)",
                        "median_price_per_m2": "Trung vị (Tr/m²)",
                        "median_price_billion": "Trung vị (Tỷ)",
                    }
                ),
                hide_index=True,
                width="stretch",
                column_config={
                    "Tỷ trọng (%)": st.column_config.NumberColumn(format="%.1f%%"),
                    "Trung vị (Tr/m²)": st.column_config.NumberColumn(format="%.1f"),
                    "Trung vị (Tỷ)": st.column_config.NumberColumn(format="%.2f"),
                },
            )

    with c2:
        st.subheader("Độ co giãn giá theo Độ rộng đường trước nhà")
        elasticity = compute_lane_width_elasticity(filtered_df)
        if elasticity["sample_count"] < 5:
            st.info("Không đủ dữ liệu `lane_width_m` hợp lệ trong tập đang lọc.")
        else:
            m1, m2, m3 = st.columns(3)
            m1.metric("Mẫu có độ rộng đường", f"{elasticity['sample_count']:,}")
            m2.metric(
                "Tương quan Pearson (r)",
                f"{elasticity['pearson_r']:.2f}" if elasticity["pearson_r"] is not None else "N/A",
            )
            m3.metric(
                "Độ nhạy (+1m đường)",
                f"{elasticity['slope_per_meter']:+.1f} Tr/m²"
                if elasticity["slope_per_meter"] is not None
                else "N/A",
            )

            bucket_df = elasticity["bucket_summary"]
            if not bucket_df.empty:
                fig_width = px.bar(
                    bucket_df,
                    x="width_bucket",
                    y="median_price_per_m2",
                    text=bucket_df["median_price_per_m2"].map(lambda v: f"{v:.1f}"),
                    color="median_price_per_m2",
                    color_continuous_scale="Blues",
                    labels={
                        "width_bucket": "Nhóm độ rộng đường",
                        "median_price_per_m2": "Trung vị đơn giá (Tr/m²)",
                    },
                )
                fig_width.update_layout(
                    height=340,
                    coloraxis_showscale=False,
                    margin=dict(t=20, b=30),
                )
                st.plotly_chart(fig_width, width="stretch")

# --- TAB 3: Area Segments, Legal Status & Liquidity ---
with tab3:
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Đơn giá & Nguồn cung theo Phân khúc Diện tích")
        seg_df = summarize_by_dimension(filtered_df, "area_segment")
        if not seg_df.empty:
            fig_seg = go.Figure()
            fig_seg.add_trace(
                go.Bar(
                    x=seg_df["area_segment"].astype(str),
                    y=seg_df["listing_count"],
                    name="Số lượng BĐS",
                    marker_color="#CBD5E1",
                    yaxis="y",
                )
            )
            fig_seg.add_trace(
                go.Scatter(
                    x=seg_df["area_segment"].astype(str),
                    y=seg_df["median_price_per_m2"],
                    name="Trung vị đơn giá (Tr/m²)",
                    mode="lines+markers+text",
                    text=seg_df["median_price_per_m2"].map(lambda v: f"{v:.1f}"),
                    textposition="top center",
                    line=dict(color="#DC2626", width=3),
                    yaxis="y2",
                )
            )
            fig_seg.update_layout(
                height=380,
                yaxis=dict(title="Số lượng BĐS"),
                yaxis2=dict(title="Đơn giá (Tr/m²)", overlaying="y", side="right"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                margin=dict(t=30, b=30),
            )
            st.plotly_chart(fig_seg, width="stretch")

    with col_b:
        st.subheader("Ảnh hưởng của Pháp lý & Nội thất lên Đơn giá")
        spec_dim = st.radio(
            "Chọn thuộc tính phân tích",
            options=["legal_status", "furniture_status"],
            format_func=lambda c: "Tình trạng pháp lý" if c == "legal_status" else "Mức độ nội thất",
            horizontal=True,
        )
        spec_label = "Tình trạng pháp lý" if spec_dim == "legal_status" else "Mức độ nội thất"
        spec_df = summarize_by_dimension(filtered_df, spec_dim).sort_values(
            "median_price_per_m2", ascending=False
        )
        if not spec_df.empty:
            fig_spec = px.bar(
                spec_df,
                x=spec_dim,
                y="median_price_per_m2",
                color=spec_dim,
                text=spec_df["median_price_per_m2"].map(lambda v: f"{v:.1f} Tr/m²"),
                labels={spec_dim: spec_label, "median_price_per_m2": "Trung vị đơn giá (Tr/m²)"},
            )
            fig_spec.update_layout(height=340, showlegend=False, margin=dict(t=20, b=30))
            st.plotly_chart(fig_spec, width="stretch")

    st.subheader("Tương quan Diện tích vs Giá niêm yết (theo Loại đường)")
    scatter_sample = filtered_df if len(filtered_df) <= 2000 else filtered_df.sample(2000, random_state=42)
    fig_scatter = px.scatter(
        scatter_sample,
        x="area_m2",
        y="price_billion_vnd",
        color="lane_type",
        size="duplicate_broker_count",
        size_max=16,
        hover_data=["district_label", "ward", "price_per_m2", "legal_status"],
        category_orders={"lane_type": LANE_TYPE_ORDER},
        labels={
            "area_m2": "Diện tích (m²)",
            "price_billion_vnd": "Giá niêm yết (Tỷ VNĐ)",
            "lane_type": "Loại đường",
            "duplicate_broker_count": "Số môi giới rao",
        },
        opacity=0.7,
    )
    fig_scatter.update_layout(height=420, margin=dict(t=20, b=30))
    st.plotly_chart(fig_scatter, width="stretch")

# --- TAB 4: Data Explorer & Export ---
with tab4:
    st.subheader(f"Danh sách {len(filtered_df):,} bất động sản đã làm sạch")

    display_cols = [
        "district_label",
        "ward",
        "street",
        "lane_type",
        "price_billion_vnd",
        "area_m2",
        "price_per_m2",
        "lane_width_m",
        "floors",
        "bedrooms",
        "legal_status",
        "duplicate_broker_count",
        "crawled_date",
        "listing_url",
    ]
    existing_cols = [c for c in display_cols if c in filtered_df.columns]

    sort_cols = [c for c in ["crawled_date", "price_billion_vnd"] if c in existing_cols]
    table_df = (
        filtered_df[existing_cols].sort_values(by=sort_cols, ascending=[False] * len(sort_cols))
        if sort_cols
        else filtered_df[existing_cols]
    )

    st.dataframe(
        table_df,
        width="stretch",
        hide_index=True,
        column_config={
            "district_label": "Quận/Huyện",
            "ward": "Phường/Xã",
            "street": "Đường",
            "lane_type": "Loại đường",
            "price_billion_vnd": st.column_config.NumberColumn("Giá (Tỷ)", format="%.2f"),
            "area_m2": st.column_config.NumberColumn("Diện tích (m²)", format="%.1f"),
            "price_per_m2": st.column_config.NumberColumn("Đơn giá (Tr/m²)", format="%.1f"),
            "lane_width_m": st.column_config.NumberColumn("Độ rộng (m)", format="%.1f"),
            "floors": st.column_config.NumberColumn("Số tầng", format="%.0f"),
            "bedrooms": st.column_config.NumberColumn("Phòng ngủ", format="%.0f"),
            "legal_status": "Pháp lý",
            "duplicate_broker_count": st.column_config.NumberColumn("Số MG rao", format="%d"),
            "crawled_date": "Ngày cào",
            "listing_url": st.column_config.LinkColumn("Link tin gốc", display_text="Xem tin"),
        },
    )

    csv_bytes = filtered_df[existing_cols].to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button(
        label="⬇️ Tải xuống dữ liệu đã lọc (CSV)",
        data=csv_bytes,
        file_name="hcmc_real_estate_filtered.csv",
        mime="text/csv",
    )

