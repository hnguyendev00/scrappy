import json
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

    df.to_csv("report.csv", index=False, encoding="utf-8")
    print(f"CSV written: report.csv ({len(df)} rows)")


if __name__ == "__main__":
    main()