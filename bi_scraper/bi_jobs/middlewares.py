"""
middlewares.py — Stealth & Anti-Detection Middleware Layer
==========================================================
Techniques used:
  • Rotating real-browser User-Agent pool
  • Randomised Accept / Accept-Language / Sec-Fetch-* headers
  • Random download delays drawn from a Gaussian distribution
  • Retry logic with exponential back-off
  • Playwright page-level stealth patches (navigator.webdriver = false, etc.)
"""

import random
import time
import logging

logger = logging.getLogger(__name__)

# ── 1.  Realistic User-Agent pool (Chrome / Firefox / Edge – various OS) ──────
USER_AGENTS = [
    # Chrome – Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    # Chrome – macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # Firefox – Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    # Firefox – Linux
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    # Safari – macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9",
    "en-US,en;q=0.8,ar;q=0.6",
    "en-US,en;q=0.9,fr;q=0.7",
]

ACCEPT_HEADERS = [
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
]


# ── 2.  Random-Delay Middleware ────────────────────────────────────────────────
class RandomDelayMiddleware:
    """
    Applies a human-like Gaussian delay before each download.
    Settings (all optional, in seconds):
        RANDOM_DELAY_MEAN   – centre of Gaussian (default 4.0)
        RANDOM_DELAY_SIGMA  – spread of Gaussian (default 2.0)
        RANDOM_DELAY_MIN    – hard floor          (default 1.5)
        RANDOM_DELAY_MAX    – hard ceiling         (default 12.0)
    """

    def __init__(self, mean, sigma, min_delay, max_delay):
        self.mean = mean
        self.sigma = sigma
        self.min_delay = min_delay
        self.max_delay = max_delay

    @classmethod
    def from_crawler(cls, crawler):
        mean      = crawler.settings.getfloat("RANDOM_DELAY_MEAN",  4.0)
        sigma     = crawler.settings.getfloat("RANDOM_DELAY_SIGMA", 2.0)
        min_delay = crawler.settings.getfloat("RANDOM_DELAY_MIN",   1.5)
        max_delay = crawler.settings.getfloat("RANDOM_DELAY_MAX",  12.0)
        return cls(mean, sigma, min_delay, max_delay)

    def process_request(self, request, spider):
        delay = random.gauss(self.mean, self.sigma)
        delay = max(self.min_delay, min(self.max_delay, delay))
        logger.debug(f"[RandomDelay] sleeping {delay:.2f}s before {request.url}")
        time.sleep(delay)


# ── 3.  Stealth-Header Middleware ──────────────────────────────────────────────
class StealthHeadersMiddleware:
    """
    Injects rotating, realistic browser headers on every outgoing request.
    Also strips headers that expose Scrapy/Python fingerprints.
    """

    SCRAPER_HEADERS_TO_DROP = {"X-Crawlera-Profile", "Accept-Charset", "Pragma"}

    def process_request(self, request, spider):
        ua = random.choice(USER_AGENTS)
        request.headers["User-Agent"]      = ua
        request.headers["Accept"]          = random.choice(ACCEPT_HEADERS)
        request.headers["Accept-Language"] = random.choice(ACCEPT_LANGUAGES)
        request.headers["Accept-Encoding"] = "gzip, deflate, br"
        request.headers["Connection"]      = "keep-alive"
        request.headers["Upgrade-Insecure-Requests"] = "1"
        request.headers["Cache-Control"]   = random.choice(["max-age=0", "no-cache"])

        # Sec-Fetch-* headers (mimic Chrome navigation)
        request.headers["Sec-Fetch-Dest"] = "document"
        request.headers["Sec-Fetch-Mode"] = "navigate"
        request.headers["Sec-Fetch-Site"] = random.choice(["none", "same-origin"])
        request.headers["Sec-Fetch-User"] = "?1"

        # Drop headers that reveal bot nature
        for h in self.SCRAPER_HEADERS_TO_DROP:
            request.headers.pop(h, None)


# ── 4.  Exponential-Backoff Retry Middleware ───────────────────────────────────
class SmartRetryMiddleware:
    """
    Retries failed / throttled requests with exponential back-off + jitter.
    Settings:
        SMART_RETRY_TIMES      – max retries per request (default 4)
        SMART_RETRY_BASE_DELAY – base delay in seconds   (default 5)
        SMART_RETRY_MAX_DELAY  – cap on delay            (default 60)
        SMART_RETRY_HTTP_CODES – status codes to retry   (default 429, 503, 403)
    """

    RETRY_COUNT_KEY = "smart_retry_count"

    def __init__(self, max_retries, base_delay, max_delay, http_codes):
        self.max_retries = max_retries
        self.base_delay  = base_delay
        self.max_delay   = max_delay
        self.http_codes  = set(http_codes)

    @classmethod
    def from_crawler(cls, crawler):
        max_retries = crawler.settings.getint("SMART_RETRY_TIMES",      4)
        base_delay  = crawler.settings.getfloat("SMART_RETRY_BASE_DELAY", 5.0)
        max_delay   = crawler.settings.getfloat("SMART_RETRY_MAX_DELAY",  60.0)
        http_codes  = crawler.settings.getlist("SMART_RETRY_HTTP_CODES", [429, 503, 403])
        http_codes  = [int(c) for c in http_codes]
        return cls(max_retries, base_delay, max_delay, http_codes)

    def process_response(self, request, response, spider):
        if response.status in self.http_codes:
            return self._retry(request, response.status, spider)
        return response

    def process_exception(self, request, exception, spider):
        return self._retry(request, type(exception).__name__, spider)

    def _retry(self, request, reason, spider):
        retries = request.meta.get(self.RETRY_COUNT_KEY, 0)
        if retries < self.max_retries:
            delay = min(
                self.base_delay * (2 ** retries) + random.uniform(0, 2),
                self.max_delay,
            )
            logger.warning(
                f"[SmartRetry] Retrying ({retries+1}/{self.max_retries}) "
                f"after {delay:.1f}s — reason: {reason} — {request.url}"
            )
            time.sleep(delay)
            new_request = request.copy()
            new_request.meta[self.RETRY_COUNT_KEY] = retries + 1
            new_request.dont_filter = True
            return new_request
        else:
            logger.error(
                f"[SmartRetry] Gave up on {request.url} after {self.max_retries} retries."
            )
            return None  # drop the request
