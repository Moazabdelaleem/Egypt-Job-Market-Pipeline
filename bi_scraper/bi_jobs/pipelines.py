"""
pipelines.py — Data Processing & Export Pipelines
===================================================
Pipeline chain (ordered by priority):
  100 – CleaningPipeline       : whitespace normalisation, field defaults
  200 – DuplicateFilterPipeline: drop duplicate URLs
  250 – CsvBackupPipeline      : local CSV backup (always works, no API needed)
  300 – GoogleSheetsPipeline   : push rows to Google Sheets (production)
"""

import os
import csv
import json
import re
import logging
import hashlib
import psycopg2
from datetime import datetime
from scrapy.exceptions import DropItem

logger = logging.getLogger(__name__)


# ── 100.  Data Cleaning ──────────────────────────────────────────────────────
class CleaningPipeline:
    """Normalise whitespace, set sane defaults, strip HTML entities."""

    @staticmethod
    def _clean(text: str) -> str:
        if not text:
            return ""
        text = re.sub(r"\s+", " ", text)   # collapse whitespace
        text = re.sub(r"&\w+;", "", text)  # strip HTML entities
        return text.strip()

    def process_item(self, item, spider):
        for field in ["title", "company", "location", "experience",
                      "job_type", "career_level", "date_posted",
                      "keywords", "description"]:
            item[field] = self._clean(item.get(field, ""))

        # Defaults
        item.setdefault("salary", "Not Specified")
        item.setdefault("scraped_at", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))

        if not item.get("title"):
            raise DropItem("Missing title — dropping item")

        return item


# ── 200.  Duplicate Filter ───────────────────────────────────────────────────
class DuplicateFilterPipeline:
    """Drop items with URLs we have already seen in this crawl session."""

    def __init__(self):
        self.seen_urls = set()

    def process_item(self, item, spider):
        url = item.get("url", "")
        if url in self.seen_urls:
            raise DropItem(f"Duplicate URL: {url}")
        self.seen_urls.add(url)
        return item


# ── 250.  Local CSV Backup ───────────────────────────────────────────────────
class CsvBackupPipeline:
    """
    Write every scraped item to a local CSV as a safety net.
    File is named:  output/bi_jobs_YYYY-MM-DD.csv
    """

    FIELD_ORDER = [
        "title", "company", "location", "experience", "job_type",
        "salary", "career_level", "date_posted", "keywords",
        "description", "url", "scraped_at",
    ]

    def open_spider(self, spider):
        os.makedirs("output", exist_ok=True)
        today = datetime.utcnow().strftime("%Y-%m-%d")
        self.filepath = f"output/bi_jobs_{today}.csv"
        file_exists = os.path.exists(self.filepath)
        self.file = open(self.filepath, "a", newline="", encoding="utf-8")
        self.writer = csv.DictWriter(
            self.file, fieldnames=self.FIELD_ORDER, extrasaction="ignore"
        )
        if not file_exists:
            self.writer.writeheader()
        logger.info(f"[CSV] Writing to {self.filepath}")

    def process_item(self, item, spider):
        self.writer.writerow(dict(item))
        return item

    def close_spider(self, spider):
        self.file.close()
        logger.info(f"[CSV] Saved backup to {self.filepath}")

# ── 260.  NLP Skill Extraction ───────────────────────────────────────────────
class SkillExtractionPipeline:
    
    # Dictionary of skills to look for. Keys are the standard name, values are regex patterns.
    SKILLS_DICT = {
        "Python": r"\bpython\b",
        "SQL": r"\bsql\b",
        "Power BI": r"\bpower\s*bi\b",
        "Tableau": r"\btableau\b",
        "Excel": r"\bexcel\b",
        "Snowflake": r"\bsnowflake\b",
        "DAX": r"\bdax\b",
        "AWS": r"\baws\b|\bamazon web services\b",
        "Azure": r"\bazure\b",
        "GCP": r"\bgcp\b|\bgoogle cloud\b",
        "Spark": r"\bspark\b|\bpyspark\b",
        "Kafka": r"\bkafka\b",
        "Airflow": r"\bairflow\b",
        "dbt": r"\bdbt\b|\bdata build tool\b",
        "Machine Learning": r"\bmachine learning\b|\bml\b",
        "ETL": r"\betl\b"
    }

    def process_item(self, item, spider):
        description = item.get('description', '').lower()
        title = item.get('title', '').lower()
        
        # Combine text to search for skills
        text_to_search = f"{title} {description}"
        
        found_skills = set()
        for skill_name, pattern in self.SKILLS_DICT.items():
            if re.search(pattern, text_to_search):
                found_skills.add(skill_name)
                
        item['skills'] = list(found_skills)
        return item
        logger.info(f"[CSV] Saved backup to {self.filepath}")



# ── 300.  PostgreSQL Cloud Export ────────────────────────────────────────────
class PostgresPipeline:
    """Upsert data into Supabase PostgreSQL"""
    
    def __init__(self, db_url):
        self.db_url = db_url

    @classmethod
    def from_crawler(cls, crawler):
        db_url = crawler.settings.get('DB_URL')
        if not db_url:
            raise ValueError("Missing DB_URL in settings. Please set the DATABASE_URL environment variable.")
        return cls(db_url=db_url)

    def open_spider(self, spider):
        try:
            self.conn = psycopg2.connect(self.db_url)
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            spider.logger.info("Connected to Supabase PostgreSQL via Pooler.")
            
            # Ensure table exists
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_postings (
                    id SERIAL PRIMARY KEY,
                    job_hash VARCHAR(255) UNIQUE NOT NULL,
                    title VARCHAR(500) NOT NULL,
                    company VARCHAR(255) NOT NULL,
                    location VARCHAR(255),
                    url TEXT,
                    skills TEXT[],
                    date_first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    date_last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status VARCHAR(50) DEFAULT 'active'
                );
            """)

            # Add skills column if it doesn't exist (in case table was made before)
            self.cursor.execute("""
                ALTER TABLE job_postings ADD COLUMN IF NOT EXISTS skills TEXT[];
            """)

            # Create Analysis Views for Power BI
            self.cursor.execute("""
                CREATE OR REPLACE VIEW v_skill_frequency AS
                SELECT unnest(skills) AS skill, COUNT(*) as frequency
                FROM job_postings
                WHERE status = 'active'
                GROUP BY skill
                ORDER BY frequency DESC;
            """)

            self.cursor.execute("""
                CREATE OR REPLACE VIEW v_hiring_trends AS
                SELECT DATE(date_first_seen) as scrape_date, COUNT(*) as new_jobs
                FROM job_postings
                GROUP BY DATE(date_first_seen)
                ORDER BY scrape_date ASC;
            """)

        except Exception as e:
            spider.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise e

    def close_spider(self, spider):
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()

    def process_item(self, item, spider):
        # Create a unique hash for the job (Company + Title + Location)
        hash_str = f"{item.get('company', '')}_{item.get('title', '')}_{item.get('location', '')}"
        job_hash = hashlib.md5(hash_str.encode('utf-8')).hexdigest()
        
        try:
            self.cursor.execute("""
                INSERT INTO job_postings (job_hash, title, company, location, url, skills, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'active')
                ON CONFLICT (job_hash) DO UPDATE 
                SET date_last_seen = CURRENT_TIMESTAMP,
                    skills = EXCLUDED.skills,
                    status = 'active';
            """, (
                job_hash,
                item.get('title', ''),
                item.get('company', ''),
                item.get('location', ''),
                item.get('url', ''),
                item.get('skills', [])
            ))
        except Exception as e:
            spider.logger.error(f"Failed to insert item into DB: {e}")

        return item
