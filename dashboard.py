import asyncio
import sqlite3
from datetime import datetime

import pandas as pd
import streamlit as st

from crawl import crawl_site_async
from json_report import write_json_report
from json_to_sqlite import json_report_to_sqlite

DB_FILE = "flights.db"
TABLE_NAME = "delta_flights"
REPORT_FILE = "report.json"


def load_data() -> pd.DataFrame:
    try:
        conn = sqlite3.connect(DB_FILE)
        df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
        conn.close()

        if df.empty:
            return df

        return make_streamlit_safe(df)
    except Exception as e:
        st.error(f"Failed to load data from SQLite: {e}")
        return pd.DataFrame()


def run_pipeline(base_url: str, max_concurrency: int, max_pages: int):
    page_data = asyncio.run(
        crawl_site_async(
            base_url=base_url,
            max_concurrency=max_concurrency,
            max_pages=max_pages,
            extract_mode="table",
        )
    )

    write_json_report(page_data, filename=REPORT_FILE)

    df, row_count = json_report_to_sqlite(
        json_file=REPORT_FILE,
        db_file=DB_FILE,
        table_name=TABLE_NAME,
    )

    if not df.empty:
        df = make_streamlit_safe(df)

    return df, row_count


def make_streamlit_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert DataFrame columns into plain pandas/Python-friendly dtypes
    so Streamlit does not choke on Arrow LargeUtf8 / extension dtypes.
    """
    safe_df = df.copy()

    for col in safe_df.columns:
        series = safe_df[col]

        # Datetime columns
        if pd.api.types.is_datetime64_any_dtype(series):
            safe_df[col] = pd.to_datetime(series, errors="coerce")
            continue

        # Numeric columns
        if pd.api.types.is_integer_dtype(series):
            safe_df[col] = pd.to_numeric(series, errors="coerce").astype("Int64")
            continue

        if pd.api.types.is_float_dtype(series):
            safe_df[col] = pd.to_numeric(series, errors="coerce")
            continue

        if pd.api.types.is_bool_dtype(series):
            safe_df[col] = series.astype("boolean")
            continue

        # Everything else becomes plain Python string/object
        safe_df[col] = series.where(series.notna(), "").map(str).astype(object)

    return safe_df


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    filtered_df = df.copy()

    st.subheader("Filters")
    col1, col2, col3 = st.columns(3)

    with col1:
        selected_origin = "All"
        if "origin" in filtered_df.columns:
            options = ["All"] + sorted(
                filtered_df["origin"]
                .replace("", pd.NA)
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )
            selected_origin = st.selectbox("Origin", options)

    with col2:
        selected_destination = "All"
        if "destination" in filtered_df.columns:
            options = ["All"] + sorted(
                filtered_df["destination"]
                .replace("", pd.NA)
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )
            selected_destination = st.selectbox("Destination", options)

    with col3:
        selected_type = "All"
        if "type" in filtered_df.columns:
            options = ["All"] + sorted(
                filtered_df["type"]
                .replace("", pd.NA)
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )
            selected_type = st.selectbox("Aircraft Type", options)

    if selected_origin != "All" and "origin" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["origin"] == selected_origin]

    if selected_destination != "All" and "destination" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["destination"] == selected_destination]

    if selected_type != "All" and "type" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["type"] == selected_type]

    return filtered_df


def show_metrics(df: pd.DataFrame) -> None:
    metric1, metric2, metric3 = st.columns(3)

    metric1.metric("Total Flights", len(df))
    metric2.metric(
        "Unique Origins",
        df["origin"].replace("", pd.NA).dropna().nunique() if "origin" in df.columns else "N/A",
    )
    metric3.metric(
        "Unique Destinations",
        df["destination"].replace("", pd.NA).dropna().nunique()
        if "destination" in df.columns
        else "N/A",
    )


def main():
    st.set_page_config(page_title="Flight Monitor", layout="wide")
    st.title("Flight Monitor")
    st.caption("Crawler → JSON → SQLite → Streamlit")

    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = None

    with st.sidebar:
        st.header("Dashboard")

        base_url = st.text_input(
            "Base URL",
            value="https://www.flightaware.com/live/fleet/DAL",
        )

        max_concurrency = st.number_input(
            "Max concurrency",
            min_value=1,
            max_value=20,
            value=2,
            step=1,
        )

        max_pages = st.number_input(
            "Max pages",
            min_value=1,
            max_value=500,
            value=20,
            step=5,
        )

        run_now = st.button("Run crawler now", use_container_width=True)
        reload_data = st.button("Reload dashboard", use_container_width=True)

    if run_now:
        with st.spinner("Crawling and loading data..."):
            try:
                _, row_count = run_pipeline(
                    base_url=base_url,
                    max_concurrency=int(max_concurrency),
                    max_pages=int(max_pages),
                )
                st.session_state.last_refresh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success(f"Refresh complete. Loaded {row_count} rows.")
            except Exception as e:
                st.error(f"Pipeline failed: {e}")

    if reload_data:
        st.rerun()

    if st.session_state.last_refresh:
        st.info(f"Last refresh: {st.session_state.last_refresh}")

    df = load_data()

    if df.empty:
        st.warning("No data found in the database yet. Run the crawler from the sidebar.")
        return

    show_metrics(df)

    filtered_df = apply_filters(df)

    st.subheader("Filtered Flights")
    st.dataframe(filtered_df, use_container_width=True)

    st.subheader("Raw Data")
    st.dataframe(df, use_container_width=True)


if __name__ == "__main__":
    main()