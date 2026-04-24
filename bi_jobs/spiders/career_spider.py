"""
career_spider.py — Company Career Pages Spider (Phase 2)
=========================================================
Reads companies_seed.json and visits each company's career page
every scheduled run to extract BI/Data job listings.

Strategy per site type:
  • Direct career pages   → CSS selectors + Playwright scroll
  • Workable ATS          → Workable's predictable HTML structure
  • Greenhouse ATS        → Greenhouse board structure
  • LinkedIn jobs         → Playwright + scroll (fallback)
  • Unknown pages         → Generic extraction heuristics

Anti-detection:
  • All middlewares from settings.py apply (delays, UA rotation, retry)
  • Per-company random idle time (simulates reading)
  • CAPTCHA detection + graceful skip
"""

import json
import os
import re
import asyncio
import random
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse

import scrapy
from bi_jobs.items import BiJobsItem

logger = logging.getLogger(__name__)

# ── BI / Data keyword filter ───────────────────────────────────────────────────
BI_KEYWORDS = [
    "data analyst", "data analytics", "business intelligence", "bi developer",
    "bi analyst", "data engineer", "data engineering", "data science",
    "data scientist", "power bi", "tableau", "etl", "data warehouse",
    "sql developer", "analytics engineer", "reporting analyst",
    "insight analyst", "dashboard", "data visualization", "ml engineer",
    "machine learning", "data infrastructure",
]

# ── ATS platform detection patterns ───────────────────────────────────────────
ATS_PATTERNS = {
    "workable":    r"apply\.workable\.com",
    "greenhouse":  r"boards(?:\.eu)?\.greenhouse\.io",
    "lever":       r"jobs\.lever\.co",
    "teamtailor":  r"teamtailor\.com",
    "recruitee":   r"recruitee\.com",
    "bamboohr":    r"bamboohr\.com",
    "linkedin":    r"linkedin\.com/jobs",
}


def detect_ats(url: str) -> str:
    """Return ATS platform name or 'generic'."""
    for name, pattern in ATS_PATTERNS.items():
        if re.search(pattern, url, re.I):
            return name
    return "generic"


def title_is_relevant(title: str) -> bool:
    # Accept all job titles for a full-market analysis
    return bool(title.strip())


class CareerSpider(scrapy.Spider):
    name = "career_spider"
    custom_settings = {
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DEPTH_LIMIT": 2,
    }

    # ── Load seed file ─────────────────────────────────────────────────────────
    def __init__(self, seed_file="companies_seed.json", *args, **kwargs):
        super().__init__(*args, **kwargs)
        seed_path = os.path.join(os.path.dirname(__file__), "..", "..", seed_file)
        seed_path = os.path.abspath(seed_path)

        if not os.path.exists(seed_path):
            raise FileNotFoundError(f"Seed file not found: {seed_path}")

        with open(seed_path, encoding="utf-8") as f:
            self.companies = json.load(f)

        logger.info(f"[Career] Loaded {len(self.companies)} companies from {seed_path}")
        self.items_scraped = 0
        self.seen_urls = set()

    # ── Generate start requests ────────────────────────────────────────────────
    def start_requests(self):
        # Filter companies that have a career page URL
        targets = [c for c in self.companies if c.get("career_page")]
        skipped = [c["name"] for c in self.companies if not c.get("career_page")]

        logger.info(
            f"[Career] {len(targets)} companies with career pages, "
            f"{len(skipped)} skipped (no URL): {', '.join(skipped)}"
        )

        for company in targets:
            url = company["career_page"].strip()
            if not url.startswith("http"):
                url = "https://" + url

            ats = detect_ats(url)
            logger.info(f"[Career] Queuing {company['name']} → {url} [{ats}]")

            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_goto_kwargs": {
                        "wait_until": "domcontentloaded",
                        "timeout": 30_000,
                    },
                    "company": company,
                    "ats": ats,
                },
                callback=self.parse_career_page,
                errback=self.errback_handler,
                dont_filter=True,
            )

    # ── Main career page parser ────────────────────────────────────────────────
    async def parse_career_page(self, response):
        page = response.meta.get("playwright_page")
        company = response.meta.get("company", {})
        ats = response.meta.get("ats", "generic")
        company_name = company.get("name", "Unknown")

        try:
            # Human-like: wait + scroll
            await page.wait_for_timeout(random.randint(2000, 4000))
            await self._human_scroll(page)
            await page.wait_for_timeout(random.randint(1500, 3000))

            if await self._is_blocked(response):
                logger.warning(f"[{company_name}] Blocked/CAPTCHA — skipping")
                return

            logger.info(f"[{company_name}] Parsing career page [{ats}]")

            # Route to the right parser based on ATS
            if ats == "workable":
                async for item in self._parse_workable(response, page, company):
                    yield item
            elif ats == "greenhouse":
                async for item in self._parse_greenhouse(response, page, company):
                    yield item
            elif ats == "lever":
                async for item in self._parse_lever(response, page, company):
                    yield item
            elif ats == "teamtailor":
                async for item in self._parse_teamtailor(response, page, company):
                    yield item
            elif ats == "recruitee":
                async for item in self._parse_recruitee(response, page, company):
                    yield item
            else:
                async for item in self._parse_generic(response, page, company):
                    yield item

        finally:
            if page:
                await page.close()

    # ══════════════════════════════════════════════════════════════════════════
    #   ATS-SPECIFIC PARSERS
    # ══════════════════════════════════════════════════════════════════════════

    async def _parse_workable(self, response, page, company):
        """Workable: apply.workable.com/<company>/"""
        # Load all jobs (click "Show more" if present)
        for _ in range(5):
            try:
                btn = await page.query_selector("button[data-ui='load-more-button']")
                if btn:
                    await btn.click()
                    await page.wait_for_timeout(1500)
                else:
                    break
            except Exception:
                break

        # Re-get response body after JS rendering
        content = await page.content()
        new_response = response.replace(body=content.encode())

        for job in new_response.css("li[class*='jobs-list-item'], div[class*='job-item']"):
            title = (
                job.css("h3[class*='job-title']::text, a[class*='title']::text").get(default="")
            ).strip()
            if not title or not title_is_relevant(title):
                continue

            location = job.css(
                "span[class*='location']::text, div[class*='location']::text"
            ).get(default="").strip()
            dept = job.css(
                "span[class*='department']::text, div[class*='department']::text"
            ).get(default="").strip()
            link = job.css("a::attr(href)").get(default="")
            url = response.urljoin(link)

            if url in self.seen_urls:
                continue
            self.seen_urls.add(url)

            yield self._make_item(title, company, location, dept, url)

    async def _parse_greenhouse(self, response, page, company):
        """Greenhouse: boards.greenhouse.io/<company>"""
        content = await page.content()
        new_response = response.replace(body=content.encode())

        for dept_section in new_response.css("section.level-0, div.opening"):
            dept = dept_section.css("h3::text, .job-department::text").get(default="").strip()
            for job in dept_section.css("div.opening, tr"):
                title = job.css("a::text").get(default="").strip()
                if not title or not title_is_relevant(title):
                    continue

                location = job.css(
                    "td.location::text, span.location::text"
                ).get(default="").strip()
                link = job.css("a::attr(href)").get(default="")
                url = response.urljoin(link)

                if url in self.seen_urls:
                    continue
                self.seen_urls.add(url)

                yield self._make_item(title, company, location, dept, url)

    async def _parse_lever(self, response, page, company):
        """Lever: jobs.lever.co/<company>"""
        content = await page.content()
        new_response = response.replace(body=content.encode())

        for posting in new_response.css("div.posting"):
            title = posting.css("h5::text").get(default="").strip()
            if not title or not title_is_relevant(title):
                continue

            location = posting.css(
                "span.sort-by-location::text, div.posting-categories .location::text"
            ).get(default="").strip()
            dept = posting.css(
                "span.sort-by-team::text, .posting-category::text"
            ).get(default="").strip()
            link = posting.css("a.posting-title::attr(href)").get(default="")
            url = link if link.startswith("http") else response.urljoin(link)

            if url in self.seen_urls:
                continue
            self.seen_urls.add(url)

            yield self._make_item(title, company, location, dept, url)

    async def _parse_teamtailor(self, response, page, company):
        """TeamTailor: <company>.teamtailor.com/jobs"""
        content = await page.content()
        new_response = response.replace(body=content.encode())

        for job in new_response.css(
            "li[class*='job-item'], div[class*='job-card'], article[class*='job']"
        ):
            title = job.css(
                "h2::text, h3::text, a[class*='title']::text, span[class*='title']::text"
            ).get(default="").strip()
            if not title or not title_is_relevant(title):
                continue

            location = job.css(
                "span[class*='location']::text, p[class*='location']::text"
            ).get(default="").strip()
            link = job.css("a::attr(href)").get(default="")
            url = response.urljoin(link)

            if url in self.seen_urls:
                continue
            self.seen_urls.add(url)

            yield self._make_item(title, company, location, "", url)

    async def _parse_recruitee(self, response, page, company):
        """Recruitee: <company>.recruitee.com"""
        content = await page.content()
        new_response = response.replace(body=content.encode())

        for job in new_response.css(
            "li.job, div[class*='offer'], div[class*='job-item']"
        ):
            title = job.css("h2::text, h3::text, a::text").get(default="").strip()
            if not title or not title_is_relevant(title):
                continue

            location = job.css(
                "li[class*='location']::text, span[class*='location']::text"
            ).get(default="").strip()
            link = job.css("a::attr(href)").get(default="")
            url = response.urljoin(link)

            if url in self.seen_urls:
                continue
            self.seen_urls.add(url)

            yield self._make_item(title, company, location, "", url)

    async def _parse_generic(self, response, page, company):
        """
        Generic heuristic parser for custom career pages.
        Looks for common job-listing patterns across many frameworks.
        """
        content = await page.content()
        new_response = response.replace(body=content.encode())

        # Try many common patterns
        job_selectors = [
            "div[class*='job-card']", "div[class*='job-listing']",
            "div[class*='position']", "li[class*='job']",
            "article[class*='job']", "tr[class*='job']",
            "div[class*='opening']", "div[class*='vacancy']",
            "div[class*='role']", "a[class*='job-title']",
            ".job", ".career-item", ".position-item",
        ]

        found_jobs = []
        for selector in job_selectors:
            jobs = new_response.css(selector)
            if jobs:
                found_jobs = jobs
                logger.info(
                    f"[{company['name']}] Matched generic selector: {selector} "
                    f"({len(jobs)} items)"
                )
                break

        if not found_jobs:
            # Last resort: find all links with BI keywords in their text
            logger.info(
                f"[{company['name']}] No job card selector matched. "
                f"Scanning all links for BI keywords..."
            )
            for link in new_response.css("a"):
                text = link.css("::text").get(default="").strip()
                href = link.attrib.get("href", "")
                if text and title_is_relevant(text) and href:
                    url = response.urljoin(href)
                    if url not in self.seen_urls:
                        self.seen_urls.add(url)
                        yield self._make_item(
                            text, company,
                            company.get("location", ""),
                            "", url
                        )
            return

        for job in found_jobs:
            # Title: try heading tags first, then any text
            title = (
                job.css("h1::text, h2::text, h3::text, h4::text").get(default="")
                or job.css("[class*='title']::text").get(default="")
                or job.css("a::text").get(default="")
            ).strip()

            if not title or not title_is_relevant(title):
                continue

            location = (
                job.css("[class*='location']::text").get(default="")
                or job.css("[class*='city']::text").get(default="")
                or company.get("location", "")
            ).strip()

            dept = (
                job.css("[class*='dept']::text, [class*='team']::text, "
                        "[class*='category']::text").get(default="")
            ).strip()

            link = job.css("a::attr(href)").get(default="")
            url = response.urljoin(link) if link else response.url

            if url in self.seen_urls:
                continue
            self.seen_urls.add(url)

            yield self._make_item(title, company, location, dept, url)

    # ══════════════════════════════════════════════════════════════════════════
    #   HELPERS
    # ══════════════════════════════════════════════════════════════════════════

    def _make_item(self, title, company, location, department, url) -> BiJobsItem:
        """Build a BiJobsItem from extracted fields."""
        self.items_scraped += 1
        item = BiJobsItem()
        item["title"]        = title
        item["company"]      = company.get("name", "")
        item["location"]     = location or company.get("location", "")
        item["experience"]   = ""
        item["job_type"]     = company.get("type", "")
        item["salary"]       = company.get("package", "Not Specified") or "Not Specified"
        item["career_level"] = ""
        item["date_posted"]  = ""
        item["keywords"]     = department or company.get("tech_field", "")
        item["description"]  = f"Domain: {company.get('domain', '')} | ATS source"
        item["url"]          = url
        item["scraped_at"]   = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        logger.info(
            f"[Scraped #{self.items_scraped}] {title} @ {company.get('name')} — {location}"
        )
        return item

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
    async def _is_blocked(response) -> bool:
        signals = [
            "captcha", "blocked", "access denied",
            "403 forbidden", "unusual traffic",
            "verify you are human", "just a moment",
        ]
        body = (response.text or "").lower()
        return any(s in body for s in signals)

    async def errback_handler(self, failure):
        company = failure.request.meta.get("company", {})
        logger.error(
            f"[Career] Failed: {company.get('name', 'Unknown')} "
            f"— {failure.request.url} — {failure.value}"
        )
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()

    def closed(self, reason):
        logger.info(
            f"[Career Spider Closed] reason={reason} | "
            f"Total BI jobs found: {self.items_scraped}"
        )
