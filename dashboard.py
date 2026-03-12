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


def load_data():
    try:
        conn = sqlite3.connect(DB_FILE)
        df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

def run_pipeline(base_url, max_concurrency, max_pages):
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
    return df, row_count


def apply_filters(df):
    filtered_df = df.copy()

    st.subheader("Filters")

    col1, col2, col3 = st.columns(3)

    with col1:
        selected_origin = "All"
        if "origin" in filtered_df.columns:
            options = ["All"] + sorted(filtered_df["origin"].dropna().astype(str).unique().tolist())
            selected_origin = st.selectbox("Origin", options)

    with col2:
        selected_destination = "All"
        if "destination" in filtered_df.columns:
            options = ["All"] + sorted(filtered_df["destination"].dropna().astype(str).unique().tolist())
            selected_destination = st.selectbox("Destination", options)

    with col3:
        selected_type = "All"
        if "type" in filtered_df.columns:
            options = ["All"] + sorted(filtered_df["type"].dropna().astype(str).unique().tolist())
            selected_type = st.selectbox("Aircraft Type", options)

    if selected_origin != "All" and "origin" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["origin"] == selected_origin]

    if selected_destination != "All" and "destination" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["destination"] == selected_destination]

    if selected_type != "All" and "type" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["type"] == selected_type]

    return filtered_df


def main():
    st.set_page_config(page_title="Flight Monitor", layout="wide")
    st.title("Flight Monitor")
    st.caption("Crawler → report.json → SQLite → Streamlit")

    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = None

    with st.sidebar:
        st.header("Crawler Controls")

        base_url = st.text_input(
            "Base URL",
            value="https://www.flightaware.com/live/fleet/DAL",
        )
        max_concurrency = st.number_input("Max concurrency", min_value=1, max_value=20, value=3)
        max_pages = st.number_input("Max pages", min_value=1, max_value=500, value=10)

        run_now = st.button("Run crawler now", use_container_width=True)
        reload_data = st.button("Reload dashboard", use_container_width=True)

    if run_now:
        with st.spinner("Crawling and loading data..."):
            try:
                df, row_count = run_pipeline(base_url, max_concurrency, max_pages)
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

    metric1, metric2, metric3 = st.columns(3)
    metric1.metric("Total Flights", len(df))
    metric2.metric("Unique Origins", df["origin"].nunique() if "origin" in df.columns else "N/A")
    metric3.metric("Unique Destinations", df["destination"].nunique() if "destination" in df.columns else "N/A")

    filtered_df = apply_filters(df)

    st.subheader("Filtered Flights")
    st.dataframe(filtered_df, use_container_width=True)

    st.subheader("Raw Data")
    st.dataframe(df, use_container_width=True)


if __name__ == "__main__":
    main()