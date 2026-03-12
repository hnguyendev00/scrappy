import json
import sqlite3
import pandas as pd


def extract_rows_from_report(json_file="report.json"):
    with open(json_file, "r", encoding="utf-8") as f:
        pages = json.load(f)

    rows = []
    for page in pages:
        rows.extend(page.get("table_rows", []))

    return rows


def build_dataframe(rows):
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).drop_duplicates()

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^\w_]", "", regex=True)
    )

    return df


def main():
    rows = extract_rows_from_report("report.json")
    df = build_dataframe(rows)

    if df.empty:
        print("No table rows found in report.json")
        return

    conn = sqlite3.connect("flights.db")
    df.to_sql("delta_flights", conn, if_exists="replace", index=False)
    conn.close()

    print(f"Loaded {len(df)} rows into flights.db -> delta_flights")


if __name__ == "__main__":
    main()