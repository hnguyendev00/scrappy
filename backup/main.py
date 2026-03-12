import sys
import asyncio
from crawl import crawl_site_async
from json_report import write_json_report

# Optional import so original code does not break
try:
    from transform import json_to_dataframe, dataframe_to_csv
except ImportError:
    json_to_dataframe = None
    dataframe_to_csv = None

async def main():
    args = sys.argv
    if len(args) < 4:
        print("usage: python main.py <base_url> <max_concurrency> <max_pages> [table]")
        sys.exit(1)

    base_url = args[1]

    if not args[2].isdigit():
        print("max_concurrency must be an integer")
        sys.exit(1)

    if not args[3].isdigit():
        print("max_pages must be an integer")
        sys.exit(1)

    max_concurrency = int(args[2])
    max_pages = int(args[3])

    extract_mode = "page"
    if len(args) >= 5:
        if args[4].lower() == "table":
            extract_mode = "table"
        else:
            print("Optional 4th argument must be: table")
            sys.exit(1)

    print(f"Starting async crawl of: {base_url}")
    print(f"Extraction mode: {extract_mode}")

    page_data = await crawl_site_async(
        base_url,
        max_concurrency,
        max_pages,
        extract_mode=extract_mode,
    )

    print(f"Crawling complete. Found {len(page_data)} pages.")

    write_json_report(page_data)

    sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())