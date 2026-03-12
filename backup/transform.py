import sqlite3
import pandas as pd
import json

with open("report.json") as f:
    pages = json.load(f)

rows = []
for page in pages:
    rows.extend(page["table_rows"])

df = pd.DataFrame(rows)

conn = sqlite3.connect("flights.db")

df.to_sql("delta_flights", conn, if_exists="replace", index=False)

conn.close()