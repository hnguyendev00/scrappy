
from urllib.parse import urlparse, urljoin
import asyncio
from bs4 import BeautifulSoup
import aiohttp


def normalize_url(url):
    parsed_url = urlparse(url)
    full_url = f"{parsed_url.netloc}{parsed_url.path}"
    if parsed_url.query:
        full_url += f"?{parsed_url.query}"
    return full_url.rstrip("/").lower()


def get_heading_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    h_tag = soup.find("h1") or soup.find("h2")
    return h_tag.get_text(strip=True) if h_tag else ""


def get_first_paragraph_from_html(html):
    soup = BeautifulSoup(html, "html.parser")

    main_section = soup.find("main")
    if main_section:
        first_p = main_section.find("p")
    else:
        first_p = soup.find("p")

    return first_p.get_text(strip=True) if first_p else ""


def get_urls_from_html(html, base_url):
    urls = []
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.find_all("a")

    for anchor in anchors:
        if href := anchor.get("href"):
            try:
                absolute_url = urljoin(base_url, href)
                urls.append(absolute_url)
            except Exception as e:
                print(f"{str(e)}: {href}")

    return urls


def get_images_from_html(html, base_url):
    image_urls = []
    soup = BeautifulSoup(html, "html.parser")
    images = soup.find_all("img")

    for img in images:
        if src := img.get("src"):
            try:
                absolute_url = urljoin(base_url, src)
                image_urls.append(absolute_url)
            except Exception as e:
                print(f"{str(e)}: {src}")

    return image_urls


def extract_flight_table(html, page_url):
    soup = BeautifulSoup(html, "html.parser")

    table = soup.find("table", class_="prettyTable")
    if not table:
        return {
            "url": page_url,
            "table_headers": [],
            "table_rows": [],
            "row_count": 0,
            "error": "Flight table with class 'prettyTable' not found",
        }

    thead = table.find("thead")
    if not thead:
        return {
            "url": page_url,
            "table_headers": [],
            "table_rows": [],
            "row_count": 0,
            "error": "Table header section not found",
        }

    header_rows = thead.find_all("tr")
    if len(header_rows) < 2:
        return {
            "url": page_url,
            "table_headers": [],
            "table_rows": [],
            "row_count": 0,
            "error": "Expected header row not found",
        }

    column_header_row = header_rows[1]
    header_cells = column_header_row.find_all("th")
    headers = [cell.get_text(" ", strip=True) for cell in header_cells]

    rows = []
    tbody = table.find("tbody")
    body_rows = tbody.find_all("tr") if tbody else table.find_all("tr")[2:]

    for tr in body_rows:
        cells = tr.find_all("td")
        if not cells:
            continue

        values = [cell.get_text(" ", strip=True) for cell in cells]

        if len(values) != len(headers):
            continue

        row = dict(zip(headers, values))
        rows.append(row)

    return {
        "url": page_url,
        "table_headers": headers,
        "table_rows": rows,
        "row_count": len(rows),
    }


def extract_page_data(html, page_url, extract_mode="page"):
    if extract_mode == "table":
        return extract_flight_table(html, page_url)

    return {
        "url": page_url,
        "heading": get_heading_from_html(html),
        "first_paragraph": get_first_paragraph_from_html(html),
        "outgoing_links": get_urls_from_html(html, page_url),
        "image_urls": get_images_from_html(html, page_url),
    }


class AsyncCrawler:
    def __init__(self, base_url, max_concurrency, max_pages, extract_mode="page"):
        self.base_url = base_url
        self.base_domain = urlparse(base_url).netloc
        self.page_data = {}
        self.lock = asyncio.Lock()
        self.max_concurrency = max_concurrency
        self.max_pages = max_pages
        self.semaphore = asyncio.Semaphore(self.max_concurrency)
        self.session = None
        self.should_stop = False
        self.all_tasks = set()
        self.extract_mode = extract_mode

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    async def add_page_visit(self, normalized_url):
        async with self.lock:
            if self.should_stop:
                return False
            if normalized_url in self.page_data:
                return False
            if len(self.page_data) >= self.max_pages:
                self.should_stop = True
                print("Reached maximum number of pages to crawl.")
                for task in self.all_tasks:
                    if not task.done():
                        task.cancel()
                return False
            return True

    async def get_html(self, url):
        try:
            async with self.session.get(
                url,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"
                    )
                },
            ) as response:
                if response.status > 399:
                    print(f"Error: HTTP {response.status} for {url}")
                    return None

                content_type = response.headers.get("content-type", "")
                if "text/html" not in content_type:
                    print(f"Error: Non-HTML content {content_type} for {url}")
                    return None

                return await response.text()
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None

    async def crawl_table_pages(self):
        offset = 0
        seen_signatures = set()

        while len(self.page_data) < self.max_pages:
            if offset == 0:
                page_url = self.base_url
            else:
                page_url = f"{self.base_url}?;offset={offset};order=ident;sort=ASC"

            print(f"Crawling table page: {page_url}")

            html = await self.get_html(page_url)
            if html is None:
                break

            page_info = extract_flight_table(html, page_url)
            rows = page_info.get("table_rows", [])

            print(f"Extracted {len(rows)} rows from {page_url}")

            if not rows:
                print(page_info.get("error", "No rows found."))
                break

            signature = tuple(tuple(sorted(r.items())) for r in rows)
            if signature in seen_signatures:
                print("Detected repeated page data. Stopping pagination.")
                break
            seen_signatures.add(signature)

            normalized_url = normalize_url(page_url)
            self.page_data[normalized_url] = page_info

            if len(rows) < 20:
                print("Last page detected.")
                break

            offset += 20

        return self.page_data

    async def crawl_page(self, current_url):
        if self.should_stop:
            return

        current_url_obj = urlparse(current_url)
        if current_url_obj.netloc != self.base_domain:
            return

        normalized_url = normalize_url(current_url)

        is_new = await self.add_page_visit(normalized_url)
        if not is_new:
            return

        async with self.semaphore:
            print(
                f"Crawling {current_url} (Active: {self.max_concurrency - self.semaphore._value})"
            )
            html = await self.get_html(current_url)
            if html is None:
                return

            page_info = extract_page_data(
                html,
                current_url,
                extract_mode=self.extract_mode,
            )

            async with self.lock:
                self.page_data[normalized_url] = page_info

            next_urls = get_urls_from_html(html, self.base_url)

        if self.should_stop:
            return

        tasks = []
        for next_url in next_urls:
            task = asyncio.create_task(self.crawl_page(next_url))
            tasks.append(task)
            self.all_tasks.add(task)

        if tasks:
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            finally:
                for task in tasks:
                    self.all_tasks.discard(task)

    async def crawl(self):
        if self.extract_mode == "table":
            return await self.crawl_table_pages()

        await self.crawl_page(self.base_url)
        return self.page_data


async def crawl_site_async(base_url, max_concurrency, max_pages, extract_mode="page"):
    async with AsyncCrawler(
        base_url,
        max_concurrency,
        max_pages,
        extract_mode=extract_mode,
    ) as crawler:
        return await crawler.crawl()