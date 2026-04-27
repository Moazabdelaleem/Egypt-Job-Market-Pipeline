"""
wuzzuf_spider.py — Wuzzuf.net Job Market Spider
=================================================
Scrapes all job listings from Wuzzuf.net (Egypt's largest job
aggregator) using paginated search results + detail page navigation
to extract full job descriptions for NLP skill extraction.

Strategy:
  • Start from the paginated search index (/search/jobs/)
  • Extract job card metadata (title, company, location, type, date)
  • Follow each job detail link to get the full description
  • NLP pipeline downstream extracts skills from the description

Anti-detection:
  • All middlewares from settings.py apply (delays, UA rotation, retry)
  • Per-page random idle time (simulates reading)
  • CAPTCHA detection + graceful skip
"""

import re
import random
import logging
from datetime import datetime
from urllib.parse import urlencode, urljoin

import scrapy
from bi_jobs.items import BiJobsItem

logger = logging.getLogger(__name__)

# ── Search queries to cover the full Egyptian job market ──────────────────────
SEARCH_QUERIES = [
    # Data & Analytics
    "data analyst",
    "data engineer",
    "data scientist",
    "business intelligence",
    "power bi",
    "machine learning",
    "data warehouse",
    "ETL",
    "tableau",
    "analytics",
    "AI engineer",
    "deep learning",
    # Software Engineering
    "software engineer",
    "backend developer",
    "frontend developer",
    "full stack developer",
    "mobile developer",
    "web developer",
    "python developer",
    "java developer",
    ".NET developer",
    "react developer",
    # DevOps & Cloud
    "devops engineer",
    "cloud engineer",
    "site reliability",
    "system administrator",
    # Product & Design
    "product manager",
    "UI UX designer",
    "project manager",
    "scrum master",
    # QA & Testing
    "QA engineer",
    "test automation",
    # Cybersecurity
    "cybersecurity",
    "information security",
    # IT & Support
    "IT support",
    "network engineer",
    "database administrator",
    # Business & Finance
    "accountant",
    "financial analyst",
    "marketing",
    "sales",
    "human resources",
    "customer service",
    "operations manager",
]

MAX_PAGES_PER_QUERY = 20  # safety cap


class WuzzufSpider(scrapy.Spider):
    name = "wuzzuf_spider"
    allowed_domains = ["wuzzuf.net"]
    custom_settings = {
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DEPTH_LIMIT": 3,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.items_scraped = 0
        self.seen_urls = set()
        self.seen_hashes = set()  # company+title+location dedup
        logger.info(
            f"[Wuzzuf] Initialised with {len(SEARCH_QUERIES)} search queries, "
            f"max {MAX_PAGES_PER_QUERY} pages each"
        )

    # ── Generate start requests ───────────────────────────────────────────────
    def start_requests(self):
        for query in SEARCH_QUERIES:
            url = f"https://wuzzuf.net/search/jobs/?q={query}&a=hpb"
            logger.info(f"[Wuzzuf] Starting search: '{query}' → {url}")

            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_goto_kwargs": {
                        "wait_until": "domcontentloaded",
                        "timeout": 30_000,
                    },
                    "query": query,
                    "page_num": 1,
                },
                callback=self.parse_listing,
                errback=self.errback_handler,
                dont_filter=True,
            )

    # ── Parse listing page ────────────────────────────────────────────────────
    async def parse_listing(self, response):
        page = response.meta.get("playwright_page")
        query = response.meta.get("query", "")
        page_num = response.meta.get("page_num", 1)

        try:
            # Human-like wait + scroll
            await page.wait_for_timeout(random.randint(1500, 3000))
            await self._human_scroll(page)
            await page.wait_for_timeout(random.randint(1000, 2000))

            if await self._is_blocked(page):
                logger.warning(f"[Wuzzuf] Blocked on '{query}' page {page_num} — skipping")
                return

            # Re-get content after JS rendering
            content = await page.content()
            new_response = response.replace(body=content.encode("utf-8"))

            # ── Extract job cards ─────────────────────────────────────────────
            # Primary selector: div.css-pkv5jc (job card container)
            cards = new_response.css("div.css-pkv5jc")
            if not cards:
                # Fallback selectors
                cards = new_response.css("div.css-1g4o566, div[class*='css-'] h2 a[href*='/jobs/p/']")

            logger.info(f"[Wuzzuf] Query '{query}' page {page_num}: {len(cards)} cards found")

            jobs_on_page = 0
            for card in cards:
                # Title + detail link
                title_link = card.css("h2 a.css-o171kl, h2 a[href*='/jobs/p/']")
                if not title_link:
                    continue

                title = title_link.css("::text").get(default="").strip()
                detail_href = title_link.attrib.get("href", "")

                if not title or not detail_href:
                    continue

                detail_url = response.urljoin(detail_href)

                # Skip if already seen
                if detail_url in self.seen_urls:
                    continue
                self.seen_urls.add(detail_url)

                # Company
                company = card.css("a.css-ipsyv7::text").get(default="").strip()
                company = company.rstrip(" -").strip()

                # Location
                location = card.css("span.css-16x61xq::text").get(default="").strip()
                # Clean location prefix
                location = re.sub(r"^Location\s*", "", location).strip()

                # Job type tags (Full Time, Part Time, etc.)
                type_tags = card.css("span.css-1lh32fc::text, div.css-1lh32fc span::text").getall()
                job_type = ", ".join([t.strip() for t in type_tags if t.strip()]) or ""

                # Date posted
                date_posted = card.css(
                    "div.css-do6t5g::text, span.css-do6t5g::text, "
                    "div.css-4c4ojb::text, time::text"
                ).get(default="").strip()

                # Build partial item, then follow detail page for full description
                meta = {
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_goto_kwargs": {
                        "wait_until": "domcontentloaded",
                        "timeout": 30_000,
                    },
                    "item_data": {
                        "title": title,
                        "company": company,
                        "location": location,
                        "job_type": job_type,
                        "date_posted": date_posted,
                        "url": detail_url,
                    },
                }

                jobs_on_page += 1
                yield scrapy.Request(
                    detail_url,
                    meta=meta,
                    callback=self.parse_detail,
                    errback=self.errback_handler,
                    dont_filter=True,
                )

            # ── Pagination ────────────────────────────────────────────────────
            if jobs_on_page > 0 and page_num < MAX_PAGES_PER_QUERY:
                next_page = page_num + 1
                next_url = (
                    f"https://wuzzuf.net/search/jobs/?q={query}&a=hpb&start={next_page - 1}"
                )
                # Check if there's actually a next page link
                has_next = new_response.css(
                    f"a[href*='start={next_page - 1}'], "
                    "nav a[aria-label='Next'], li.next a"
                )
                if has_next:
                    logger.info(f"[Wuzzuf] Following to page {next_page} for '{query}'")
                    yield scrapy.Request(
                        next_url,
                        meta={
                            "playwright": True,
                            "playwright_include_page": True,
                            "playwright_page_goto_kwargs": {
                                "wait_until": "domcontentloaded",
                                "timeout": 30_000,
                            },
                            "query": query,
                            "page_num": next_page,
                        },
                        callback=self.parse_listing,
                        errback=self.errback_handler,
                        dont_filter=True,
                    )
                else:
                    logger.info(f"[Wuzzuf] No more pages for '{query}' (stopped at page {page_num})")
            else:
                logger.info(f"[Wuzzuf] Finished query '{query}' at page {page_num}")

        finally:
            if page:
                await page.close()

    # ── Parse detail page ─────────────────────────────────────────────────────
    async def parse_detail(self, response):
        page = response.meta.get("playwright_page")
        item_data = response.meta.get("item_data", {})

        try:
            # Human-like wait
            await page.wait_for_timeout(random.randint(1000, 2500))

            content = await page.content()
            new_response = response.replace(body=content.encode("utf-8"))

            # ── Extract full description ──────────────────────────────────────
            # Job Description block
            desc_sections = new_response.css("div.css-n7fcne, div.css-5pnqc5")
            description_parts = []
            for section in desc_sections:
                text = section.css("::text").getall()
                description_parts.extend([t.strip() for t in text if t.strip()])
            description = " ".join(description_parts)

            # Requirements block (often a separate section)
            req_sections = new_response.css("div.css-1lqavbg")
            requirements_parts = []
            for section in req_sections:
                text = section.css("::text").getall()
                requirements_parts.extend([t.strip() for t in text if t.strip()])
            requirements = " ".join(requirements_parts)

            # Combine description + requirements for skill extraction
            full_text = f"{description} {requirements}".strip()
            if not full_text:
                # Fallback: grab all text from the main content area
                full_text = " ".join(
                    new_response.css("section ::text, article ::text").getall()
                )

            # ── Extract additional metadata ───────────────────────────────────
            # Experience level
            experience = ""
            exp_match = new_response.css(
                "div.css-rcl8e5 span::text, "
                "span[class*='experience']::text"
            ).getall()
            for e in exp_match:
                if "yr" in e.lower() or "experience" in e.lower() or "entry" in e.lower():
                    experience = e.strip()
                    break

            # Career level
            career_level = ""
            level_texts = new_response.css(
                "div.css-rcl8e5 span::text, span.css-1k5ee52::text"
            ).getall()
            for lt in level_texts:
                lt_clean = lt.strip().lower()
                if any(kw in lt_clean for kw in [
                    "entry", "junior", "mid", "senior", "manager", "director",
                    "experienced", "student", "fresh"
                ]):
                    career_level = lt.strip()
                    break

            # Salary
            salary = "Not Specified"
            salary_el = new_response.css(
                "span[class*='salary']::text, div[class*='salary']::text"
            ).get(default="")
            if salary_el and salary_el.strip():
                salary = salary_el.strip()

            # Keywords / tags
            keywords_list = new_response.css(
                "a.css-1jf4wgr::text, a[class*='tag']::text, "
                "span.css-1ebpr::text"
            ).getall()
            keywords = ", ".join([k.strip() for k in keywords_list if k.strip()])

            # ── Dedup check ───────────────────────────────────────────────────
            dedup_key = f"{item_data.get('company', '')}_{item_data.get('title', '')}_{item_data.get('location', '')}"
            if dedup_key in self.seen_hashes:
                return
            self.seen_hashes.add(dedup_key)

            # ── Build final item ──────────────────────────────────────────────
            self.items_scraped += 1
            item = BiJobsItem()
            item["title"] = item_data.get("title", "")
            item["company"] = item_data.get("company", "")
            item["location"] = item_data.get("location", "")
            item["experience"] = experience
            item["job_type"] = item_data.get("job_type", "")
            item["salary"] = salary
            item["career_level"] = career_level
            item["date_posted"] = item_data.get("date_posted", "")
            item["keywords"] = keywords
            item["description"] = full_text[:5000]  # cap at 5000 chars
            item["url"] = item_data.get("url", response.url)
            item["scraped_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            logger.info(
                f"[Scraped #{self.items_scraped}] {item['title']} @ {item['company']} "
                f"— {item['location']} — {len(item.get('description', ''))} chars desc"
            )
            yield item

        finally:
            if page:
                await page.close()

    # ══════════════════════════════════════════════════════════════════════════
    #   HELPERS
    # ══════════════════════════════════════════════════════════════════════════

    async def _human_scroll(self, page):
        """Scroll page in human-like increments."""
        try:
            total = await page.evaluate("document.body.scrollHeight")
            pos = 0
            while pos < total:
                step = random.randint(300, 700)
                pos = min(pos + step, total)
                await page.evaluate(f"window.scrollTo(0, {pos})")
                await page.wait_for_timeout(random.randint(300, 700))
        except Exception:
            pass

    @staticmethod
    async def _is_blocked(page) -> bool:
        try:
            content = await page.content()
            body = content.lower()
            signals = [
                "captcha", "blocked", "access denied",
                "403 forbidden", "unusual traffic",
                "verify you are human", "just a moment",
            ]
            return any(s in body for s in signals)
        except Exception:
            return False

    async def errback_handler(self, failure):
        logger.error(f"[Wuzzuf] Request failed: {failure.request.url} — {failure.value}")
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()

    def closed(self, reason):
        logger.info(
            f"[Wuzzuf Spider Closed] reason={reason} | "
            f"Total jobs scraped: {self.items_scraped}"
        )
