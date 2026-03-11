import json


def write_json_report(page_data, filename="report.json"):
    if not page_data:
        print("No data to write to JSON")
        return

    pages = list(page_data.values())
    pages.sort(key=lambda p: p["url"])

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(pages, f, indent=2)

    print(f"Report written to {filename}")
