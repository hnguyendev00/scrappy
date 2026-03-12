import sqlite3
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt


DB_FILE = "flights.db"
TABLE_NAME = "delta_flights"


@st.cache_data
def load_data():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
    conn.close()
    return df


def main():
    st.set_page_config(page_title="Flight Monitor", layout="wide")
    st.title("Flight Monitor")

    df = load_data()

    if df.empty:
        st.warning("No data found in the database.")
        return

    st.subheader("Raw Data")
    st.dataframe(df, use_container_width=True)

    st.subheader("Filters")

    col1, col2, col3 = st.columns(3)

    with col1:
        if "origin" in df.columns:
            origin_options = ["All"] + sorted(df["origin"].dropna().unique().tolist())
            selected_origin = st.selectbox("Origin", origin_options)
        else:
            selected_origin = "All"

    with col2:
        if "destination" in df.columns:
            destination_options = ["All"] + sorted(df["destination"].dropna().unique().tolist())
            selected_destination = st.selectbox("Destination", destination_options)
        else:
            selected_destination = "All"

    with col3:
        if "type" in df.columns:
            type_options = ["All"] + sorted(df["type"].dropna().unique().tolist())
            selected_type = st.selectbox("Aircraft Type", type_options)
        else:
            selected_type = "All"

    filtered_df = df.copy()

    if selected_origin != "All" and "origin" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["origin"] == selected_origin]

    if selected_destination != "All" and "destination" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["destination"] == selected_destination]

    if selected_type != "All" and "type" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["type"] == selected_type]

    st.subheader("Filtered Flights")
    st.dataframe(filtered_df, use_container_width=True)

    metric1, metric2, metric3 = st.columns(3)
    metric1.metric("Total Flights", len(filtered_df))

    if "origin" in filtered_df.columns:
        metric2.metric("Unique Origins", filtered_df["origin"].nunique())
    else:
        metric2.metric("Unique Origins", "N/A")

    if "destination" in filtered_df.columns:
        metric3.metric("Unique Destinations", filtered_df["destination"].nunique())
    else:
        metric3.metric("Unique Destinations", "N/A")




if __name__ == "__main__":
    main()