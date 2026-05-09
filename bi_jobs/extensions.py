"""
extensions.py — Custom Scrapy Extensions
==========================================
NormalizationExtension:
    Listens for spider_closed signal and automatically runs the
    Bronze → Silver normalization pipeline after every crawl.
"""

import logging
import psycopg2
from scrapy import signals
from scrapy.exceptions import NotConfigured

logger = logging.getLogger(__name__)


class NormalizationExtension:
    """
    Scrapy extension that triggers the Silver normalization layer
    automatically after every crawl finishes.
    """

    def __init__(self, db_url: str):
        self.db_url = db_url

    @classmethod
    def from_crawler(cls, crawler):
        db_url = crawler.settings.get("DATABASE_URL")
        if not db_url:
            raise NotConfigured("DATABASE_URL not set — NormalizationExtension disabled")

        ext = cls(db_url)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_closed(self, spider, reason):
        """Runs after every crawl, regardless of how it ended."""
        logger.info(f"[NormalizationExtension] Spider closed (reason={reason}). Starting Silver normalization...")

        if reason not in ("finished", "shutdown", "cancelled"):
            logger.warning(
                f"[NormalizationExtension] Spider closed with reason '{reason}' — "
                "running normalization anyway to capture partial data."
            )

        try:
            # Import here to avoid circular imports
            from bi_jobs.normalize import run_normalization

            conn = psycopg2.connect(self.db_url)
            n = run_normalization(conn=conn)
            logger.info(f"[NormalizationExtension] Silver layer updated — {n} rows normalized.")

        except Exception as e:
            logger.error(f"[NormalizationExtension] Normalization failed: {e}")
            # Never crash the spider process over normalization errors
