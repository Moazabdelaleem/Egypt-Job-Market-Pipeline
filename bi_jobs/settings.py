BOT_NAME = "bi_jobs"

SPIDER_MODULES = ["bi_jobs.spiders"]
NEWSPIDER_MODULE = "bi_jobs.spiders"

# ══════════════════════════════════════════════════════════════════════════════
#   ANTI-BAN & STEALTH SETTINGS
# ══════════════════════════════════════════════════════════════════════════════

ROBOTSTXT_OBEY = False              # we need to access pages robots.txt blocks
USER_AGENT = None                   # handled by StealthHeadersMiddleware
COOKIES_ENABLED = False             # avoid tracking cookies
TELNETCONSOLE_ENABLED = False       # no open ports

# Base Scrapy download delay (on top of our RandomDelayMiddleware)
DOWNLOAD_DELAY = 2
RANDOMIZE_DOWNLOAD_DELAY = True

# Throttle concurrency to look like a single user
CONCURRENT_REQUESTS = 1
CONCURRENT_REQUESTS_PER_DOMAIN = 1
# CONCURRENT_REQUESTS_PER_IP = 1

# Auto-throttle: dynamically adjusts delay based on server load
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 3
AUTOTHROTTLE_MAX_DELAY = 15
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
AUTOTHROTTLE_DEBUG = False

# Timeout (seconds)
DOWNLOAD_TIMEOUT = 30

# ── RandomDelayMiddleware Tuning ──────────────────────────────────────────────
RANDOM_DELAY_MEAN  = 4.0    # centre of Gaussian (seconds)
RANDOM_DELAY_SIGMA = 2.0    # spread
RANDOM_DELAY_MIN   = 1.5    # floor
RANDOM_DELAY_MAX   = 12.0   # ceiling

# ── SmartRetryMiddleware Tuning ───────────────────────────────────────────────
SMART_RETRY_TIMES      = 4
SMART_RETRY_BASE_DELAY = 5.0
SMART_RETRY_MAX_DELAY  = 60.0
SMART_RETRY_HTTP_CODES = [429, 503, 403, 520, 521, 522, 524]

# ══════════════════════════════════════════════════════════════════════════════
#   PLAYWRIGHT INTEGRATION
# ══════════════════════════════════════════════════════════════════════════════

DOWNLOAD_HANDLERS = {
    "http":  "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "args": [
        "--disable-blink-features=AutomationControlled",
        "--disable-infobars",
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-accelerated-2d-canvas",
        "--disable-gpu",
        "--window-size=1920,1080",
        "--lang=en-US,en",
    ],
}

# Inject stealth JS into every new Playwright page automatically
PLAYWRIGHT_CONTEXTS = {
    "default": {
        "viewport": {"width": 1920, "height": 1080},
        "locale": "en-US",
        "timezone_id": "Africa/Cairo",
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "java_script_enabled": True,
        "ignore_https_errors": True,
    }
}

# ══════════════════════════════════════════════════════════════════════════════
#   DOWNLOADER MIDDLEWARES  (order matters – lower number runs first)
# ══════════════════════════════════════════════════════════════════════════════

DOWNLOADER_MIDDLEWARES = {
    # Disable the default UA middleware (we replace it)
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    # Disable default retry (we have our own)
    "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,

    # ── Our stealth stack ──
    "bi_jobs.middlewares.StealthHeadersMiddleware": 400,
    "bi_jobs.middlewares.RandomDelayMiddleware":    500,
    "bi_jobs.middlewares.SmartRetryMiddleware":     550,
}

# ══════════════════════════════════════════════════════════════════════════════
#   ITEM PIPELINES
# ══════════════════════════════════════════════════════════════════════════════

import os

ITEM_PIPELINES = {
    "bi_jobs.pipelines.CleaningPipeline":       100,
    "bi_jobs.pipelines.DuplicateFilterPipeline": 200,
    "bi_jobs.pipelines.CsvBackupPipeline":       250,
    "bi_jobs.pipelines.SkillExtractionPipeline": 260,
    "bi_jobs.pipelines.PostgresPipeline":        300,
}

DB_URL = os.environ.get('DATABASE_URL')

# ══════════════════════════════════════════════════════════════════════════════
#   GENERAL SCRAPY
# ══════════════════════════════════════════════════════════════════════════════

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
LOG_DATEFORMAT = "%Y-%m-%d %H:%M:%S"
