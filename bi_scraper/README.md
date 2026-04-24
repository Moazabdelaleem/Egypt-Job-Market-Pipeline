# 📊 Egyptian Job Market BI Pipeline

An automated, end-to-end data engineering pipeline designed to harvest Business Intelligence, Data Science, and Data Engineering internship postings from 83 target companies in Egypt.

The system extracts unstructured data from heterogeneous applicant tracking systems (Workable, Lever, Greenhouse, etc.), processes the text using NLP to extract technical skills, and continuously upserts the structured data into a cloud PostgreSQL database to power a live Power BI dashboard.

---

## 🏗️ Architecture & Data Flow

1. **Extraction (`career_spider.py`)**
   * Uses Scrapy and Playwright to stealthily crawl company career pages.
   * Features dynamic ATS detection to route HTML to bespoke parsers.
   * Filters specifically for "Intern" and "Internship" roles.

2. **NLP Processing (`SkillExtractionPipeline`)**
   * Processes the raw job descriptions.
   * Utilizes regex heuristics to extract specific in-demand technical skills (e.g., Python, SQL, Power BI, AWS, dbt).

3. **Database Integration (`PostgresPipeline`)**
   * Connects securely to a Supabase PostgreSQL instance via connection pooling.
   * Auto-generates the `job_postings` table and analytical SQL Views.
   * Performs an `UPSERT` using a generated `job_hash` to maintain a rolling window of active jobs, updating `date_last_seen` to track hiring velocity.

---

## 🥷 Stealth & Anti-Bot Measures

To avoid 403 Forbidden errors and CAPTCHAs, the pipeline employs a robust stealth middleware stack (`middlewares.py`):

1. **Playwright Stealth Patches:** Strips automation markers like `navigator.webdriver`.
2. **Dynamic Header Injection:** Fakes human-like `Sec-Fetch-Dest` and `Accept-Language` headers.
3. **Rotating User-Agents:** Draws from a curated pool of realistic Chrome, Edge, and Firefox strings.
4. **Gaussian Random Delays:** Mimics human reading speed by drawing sleep intervals from a Gaussian distribution.
5. **Human-like Interaction:** Automatically simulates randomized scrolling to trigger lazy-loaded elements.
6. **Exponential Backoff:** Gracefully handles rate limits (HTTP 429) and server overloads (HTTP 503).

---

## 📂 Project Structure

```text
bi_scraper/
├── .github/workflows/
│   └── scraper.yml              # CI/CD pipeline for automated scraping
├── bi_jobs/
│   ├── spiders/
│   │   └── career_spider.py     # Main ATS & Career site spider
│   ├── items.py                 # Scrapy Item definition
│   ├── middlewares.py           # Stealth, U/A rotation, and delays
│   ├── pipelines.py             # NLP extraction & PostgreSQL integration
│   └── settings.py              # Playwright config & pipeline priorities
├── companies_seed.json          # Master list of 83 target companies
└── requirements.txt             # Python dependencies
```

---

## 🚀 Setup & Execution

### Prerequisites
* Python 3.10+
* Playwright browsers
* A PostgreSQL Database URL (configured as `DB_URL` environment variable)

### Installation
```bash
pip install -r requirements.txt
playwright install chromium
playwright install-deps
```

### Running Locally
```bash
# Ensure your DB_URL is set in settings.py or exported in your terminal
scrapy crawl career_spider
```

---

## ⚙️ Automation (GitHub Actions)

The pipeline is fully automated via GitHub Actions (`scraper.yml`) with zero-cost execution.
* **Schedule:** Runs automatically every 2 days.
* **Database Sync:** Connects to the cloud PostgreSQL database and upserts the latest job market state, ensuring the Power BI dashboard is always live.
