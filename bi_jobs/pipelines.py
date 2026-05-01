"""
pipelines.py — Data Processing & Export Pipelines
===================================================
Pipeline chain (ordered by priority):
  100 – CleaningPipeline       : whitespace normalisation, field defaults,
                                  title scrubbing, non-job filtering
  200 – DuplicateFilterPipeline: drop duplicate URLs
  250 – CsvBackupPipeline      : local CSV backup (always works, no API needed)
  260 – SkillExtractionPipeline: NLP regex skill tagging
  300 – PostgresPipeline       : upsert to Supabase PostgreSQL
"""

import os
import csv
import json
import re
import logging
import hashlib
import psycopg2
from datetime import datetime, timedelta
from scrapy.exceptions import DropItem

logger = logging.getLogger(__name__)


# ── 100.  Data Cleaning ──────────────────────────────────────────────────────
class CleaningPipeline:
    """
    Normalise whitespace, scrub concatenated metadata from titles,
    filter out non-job entries, and clean locations.
    """

    # Patterns that are concatenated into titles by bad HTML extraction
    TITLE_JUNK_PATTERNS = [
        r"On-site",
        r"Hybrid",
        r"Remote",
        r"Full-time",
        r"Part-time",
        r"Freelance\s*/?\s*Project",
        r"Contract",
        r"Internship",
        r"More\s*Details?",
        r"·",  # bullet separator
        r"�",  # encoding artefact
    ]

    # Known non-job titles (UI elements, nav links, marketing copy)
    BLACKLIST_TITLES = {
        "home", "careers", "career", "social", "expertise", "privacy policy",
        "powered bygdpr cookie compliance", "connectivity", "tech transformation",
        "intelligent content management", "read more", "apply now", "apply",
        "meet bmbotin our immersive reception area!",
        "find the perfect opening and apply with a click",
        "view all jobs", "see all jobs", "load more", "show more",
        "cookie policy", "terms of use", "about us", "contact us",
        "join us", "our team", "our culture", "our values",
    }

    # Minimum title length after cleaning (filters out junk like "IT", single words)
    MIN_TITLE_LENGTH = 5

    @staticmethod
    def _clean(text: str) -> str:
        if not text:
            return ""
        text = re.sub(r"\s+", " ", text)   # collapse whitespace
        text = re.sub(r"&\w+;", "", text)  # strip HTML entities
        return text.strip()

    def _scrub_title(self, title: str) -> str:
        """Remove concatenated metadata junk from job titles."""
        if not title:
            return ""

        # Build a combined regex from all junk patterns
        for pattern in self.TITLE_JUNK_PATTERNS:
            # Split on the junk pattern and keep only the part before it
            parts = re.split(pattern, title, flags=re.IGNORECASE)
            title = parts[0].strip()

        # Remove trailing city/country names that got concatenated
        # (e.g., "Senior AnalystCairo" → "Senior Analyst")
        # Look for a capital letter that starts a known city after the real title
        title = re.sub(
            r"(Cairo|Egypt|Giza|Maadi|Alexandria|New Cairo City|Nasr City|"
            r"Lebanon|Iraq|KSA|Dubai|UAE|Saudi Arabia|Riyadh|Jeddah|"
            r"Al Minufiyah|Benha|Port Said|Heliopolis|Mansoura|"
            r"6th of October|Sheikh Zayed|Obour|Qalyoub|Dokki|Mohandessin)$",
            "", title, flags=re.IGNORECASE
        ).strip()

        # Remove trailing company division names (e.g., "BMB Drive", "BMB Reach")
        title = re.sub(r"\s*(BMB\s+\w+|More\s+Detail)$", "", title, flags=re.IGNORECASE).strip()

        return title

    def _clean_location(self, location: str) -> str:
        """Normalize location field."""
        if not location:
            return ""
        # Strip "Location" prefix
        location = re.sub(r"^Location\s*", "", location, flags=re.IGNORECASE).strip()
        # Normalize separators
        location = re.sub(r"\s*[-–—]\s*", ", ", location)
        return location.strip(", ")

    def _is_non_job(self, title: str) -> bool:
        """Check if a 'title' is actually a nav link or UI element."""
        title_lower = title.lower().strip()
        if title_lower in self.BLACKLIST_TITLES:
            return True
        if len(title_lower) < self.MIN_TITLE_LENGTH:
            return True
        return False

    def _calculate_historical_date(self, date_posted: str, scraped_at: str) -> str:
        """Parse raw '2 months ago' to a YYYY-MM-DD date."""
        if not date_posted:
            return ""
        
        try:
            base_date = datetime.strptime(scraped_at, "%Y-%m-%d %H:%M:%S")
        except:
            base_date = datetime.utcnow()

        text = date_posted.lower()
        num_match = re.search(r'\d+', text)
        num = int(num_match.group()) if num_match else 1

        if 'hour' in text or 'minute' in text or 'second' in text or 'moment' in text:
            hist_date = base_date
        elif 'day' in text:
            hist_date = base_date - timedelta(days=num)
        elif 'month' in text:
            hist_date = base_date - timedelta(days=num*30)
        elif 'year' in text:
            hist_date = base_date - timedelta(days=num*365)
        elif 'yesterday' in text:
            hist_date = base_date - timedelta(days=1)
        else:
            hist_date = base_date
            
        return hist_date.strftime("%Y-%m-%d")

    def process_item(self, item, spider):
        # Basic cleaning on all text fields
        for field in ["title", "company", "location", "experience",
                      "job_type", "career_level", "date_posted",
                      "keywords", "description"]:
            item[field] = self._clean(item.get(field, ""))

        # Scrub the title
        item["title"] = self._scrub_title(item["title"])

        # Clean location
        item["location"] = self._clean_location(item["location"])

        # Filter out non-job entries
        if self._is_non_job(item.get("title", "")):
            raise DropItem(f"Non-job entry: '{item.get('title', '')}'")

        # Defaults
        item.setdefault("salary", "Not Specified")
        item.setdefault("scraped_at", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        item["historical_date"] = self._calculate_historical_date(item.get("date_posted", ""), item["scraped_at"])

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
        "description", "url", "scraped_at", "skills",
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
        # Programming Languages
        "Python": r"\bpython\b",
        "SQL": r"\bsql\b",
        "Java": r"\bjava\b(?!script)",
        "JavaScript": r"\bjavascript\b|\bjs\b",
        "TypeScript": r"\btypescript\b|\bts\b",
        "C#": r"\bc#\b|\.net",
        "C++": r"\bc\+\+\b",
        "PHP": r"\bphp\b",
        "Ruby": r"\bruby\b",
        "Go": r"\bgolang\b|\bgo\b(?:\s+lang)",
        "Scala": r"\bscala\b",
        "R": r"\br programming\b|\br studio\b|\brstudio\b",
        "Swift": r"\bswift\b",
        "Kotlin": r"\bkotlin\b",
        # Data & BI Tools
        "Power BI": r"\bpower\s*bi\b",
        "Tableau": r"\btableau\b",
        "Excel": r"\bexcel\b",
        "Looker": r"\blooker\b",
        "DAX": r"\bdax\b",
        "SSIS": r"\bssis\b",
        "SSRS": r"\bssrs\b",
        "SSAS": r"\bssas\b",
        # Data Engineering
        "Spark": r"\bspark\b|\bpyspark\b",
        "Kafka": r"\bkafka\b",
        "Airflow": r"\bairflow\b",
        "dbt": r"\bdbt\b|\bdata build tool\b",
        "ETL": r"\betl\b",
        "Snowflake": r"\bsnowflake\b",
        "Databricks": r"\bdatabricks\b",
        "Hadoop": r"\bhadoop\b",
        # Cloud Platforms
        "AWS": r"\baws\b|\bamazon web services\b",
        "Azure": r"\bazure\b",
        "GCP": r"\bgcp\b|\bgoogle cloud\b",
        # DevOps & Infrastructure
        "Docker": r"\bdocker\b",
        "Kubernetes": r"\bkubernetes\b|\bk8s\b",
        "Terraform": r"\bterraform\b",
        "CI/CD": r"\bci\s*/?\s*cd\b|\bjenkins\b|\bgithub actions\b",
        "Linux": r"\blinux\b|\bubuntu\b|\bcentos\b",
        "Git": r"\bgit\b|\bgithub\b|\bgitlab\b",
        # Databases
        "PostgreSQL": r"\bpostgres\b|\bpostgresql\b",
        "MySQL": r"\bmysql\b",
        "MongoDB": r"\bmongodb\b|\bmongo\b",
        "Redis": r"\bredis\b",
        "Oracle": r"\boracle\b",
        "NoSQL": r"\bnosql\b|\bcassandra\b|\bdynamodb\b",
        # Frontend & Frameworks
        "React": r"\breact\b|\breactjs\b",
        "Angular": r"\bangular\b",
        "Vue.js": r"\bvue\b|\bvuejs\b",
        "Node.js": r"\bnode\.?js\b|\bnode\b",
        "Django": r"\bdjango\b",
        "Flask": r"\bflask\b",
        "Spring": r"\bspring\s*boot\b|\bspring\b",
        # ML & AI
        "Machine Learning": r"\bmachine learning\b|\bml\b",
        "Deep Learning": r"\bdeep learning\b|\bneural network\b",
        "TensorFlow": r"\btensorflow\b",
        "PyTorch": r"\bpytorch\b",
        "NLP": r"\bnlp\b|\bnatural language\b",
        "Computer Vision": r"\bcomputer vision\b|\bcv\b|\bimage recognition\b",
        # Project Management & Collaboration
        "Agile": r"\bagile\b|\bscrum\b|\bkanban\b",
        "Jira": r"\bjira\b",
        "SAP": r"\bsap\b",
        "Salesforce": r"\bsalesforce\b",
        # Design
        "Figma": r"\bfigma\b",
        "Adobe": r"\badobe\b|\bphotoshop\b|\billustrator\b",
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
            
            # Ensure table exists with ALL fields
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_postings (
                    id SERIAL PRIMARY KEY,
                    job_hash VARCHAR(255) UNIQUE NOT NULL,
                    title VARCHAR(500) NOT NULL,
                    company VARCHAR(255) NOT NULL,
                    location VARCHAR(255),
                    experience VARCHAR(255),
                    job_type VARCHAR(255),
                    salary VARCHAR(255) DEFAULT 'Not Specified',
                    career_level VARCHAR(255),
                    date_posted VARCHAR(255),
                    keywords TEXT,
                    description TEXT,
                    url TEXT,
                    skills TEXT[],
                    date_first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    date_last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status VARCHAR(50) DEFAULT 'active'
                );
            """)

            # Add columns if they don't exist (migration for existing tables)
            migration_cols = [
                ("experience", "VARCHAR(255)"),
                ("job_type", "VARCHAR(255)"),
                ("salary", "VARCHAR(255) DEFAULT 'Not Specified'"),
                ("career_level", "VARCHAR(255)"),
                ("date_posted", "VARCHAR(255)"),
                ("keywords", "TEXT"),
                ("description", "TEXT"),
                ("skills", "TEXT[]"),
                ("historical_date", "DATE"),
            ]
            for col_name, col_type in migration_cols:
                self.cursor.execute(f"""
                    ALTER TABLE job_postings ADD COLUMN IF NOT EXISTS {col_name} {col_type};
                """)

            # ── Relational Views for Power BI (Star Schema) ──────────────────
            # This allows Power BI to import these as related tables without Power Query

            self.cursor.execute("""
                CREATE OR REPLACE VIEW v_job_types_expanded AS
                SELECT job_hash, TRIM(unnest(string_to_array(job_type, ','))) AS job_type_individual
                FROM job_postings
                WHERE status = 'active' AND job_type IS NOT NULL AND job_type != '';
            """)

            self.cursor.execute("""
                CREATE OR REPLACE VIEW v_job_skills_expanded AS
                SELECT job_hash, unnest(skills) AS skill
                FROM job_postings
                WHERE status = 'active' AND skills IS NOT NULL;
            """)

            # ── Analysis Views for Power BI ──────────────────────────────────
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

            self.cursor.execute("""
                CREATE OR REPLACE VIEW v_top_companies AS
                SELECT company, COUNT(*) as open_positions
                FROM job_postings
                WHERE status = 'active'
                GROUP BY company
                ORDER BY open_positions DESC;
            """)

            self.cursor.execute("""
                CREATE OR REPLACE VIEW v_job_type_breakdown AS
                SELECT job_type, COUNT(*) as count
                FROM job_postings
                WHERE status = 'active' AND job_type IS NOT NULL AND job_type != ''
                GROUP BY job_type
                ORDER BY count DESC;
            """)

            self.cursor.execute("""
                CREATE OR REPLACE VIEW v_career_level_distribution AS
                SELECT career_level, COUNT(*) as count
                FROM job_postings
                WHERE status = 'active' AND career_level IS NOT NULL AND career_level != ''
                GROUP BY career_level
                ORDER BY count DESC;
            """)

            self.cursor.execute("""
                CREATE OR REPLACE VIEW v_location_distribution AS
                SELECT location, COUNT(*) as count
                FROM job_postings
                WHERE status = 'active' AND location IS NOT NULL AND location != ''
                GROUP BY location
                ORDER BY count DESC;
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
                INSERT INTO job_postings (
                    job_hash, title, company, location, experience, job_type,
                    salary, career_level, date_posted, keywords, description,
                    url, skills, status, historical_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'active', %s)
                ON CONFLICT (job_hash) DO UPDATE 
                SET date_last_seen = CURRENT_TIMESTAMP,
                    skills = EXCLUDED.skills,
                    description = EXCLUDED.description,
                    salary = EXCLUDED.salary,
                    career_level = EXCLUDED.career_level,
                    experience = EXCLUDED.experience,
                    job_type = EXCLUDED.job_type,
                    historical_date = EXCLUDED.historical_date,
                    status = 'active';
            """, (
                job_hash,
                item.get('title', ''),
                item.get('company', ''),
                item.get('location', ''),
                item.get('experience', ''),
                item.get('job_type', ''),
                item.get('salary', 'Not Specified'),
                item.get('career_level', ''),
                item.get('date_posted', ''),
                item.get('keywords', ''),
                item.get('description', ''),
                item.get('url', ''),
                item.get('skills', []),
                item.get('historical_date', None)
            ))
        except Exception as e:
            spider.logger.error(f"Failed to insert item into DB: {e}")

        return item

