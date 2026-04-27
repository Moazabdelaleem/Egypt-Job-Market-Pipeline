# 📊 Egypt Job Market Pipeline (Wuzzuf)

An automated, end-to-end data engineering pipeline designed to harvest Business Intelligence, Data Science, and Data Engineering job postings from Wuzzuf, Egypt's leading job platform.

The system extracts unstructured data, processes the text using NLP to extract technical skills, and continuously upserts the structured data into a cloud PostgreSQL database to power a live Power BI dashboard.

---

## 🏗️ Architecture & Data Flow

1. **Extraction (`wuzzuf_spider.py`)**
   * Uses Scrapy and Playwright to crawl Wuzzuf.
   * Targets key roles: Data Analyst, Data Scientist, Data Engineer, BI Developer, etc.
   * Handles pagination and dynamic content rendering.

2. **NLP Processing (`SkillExtractionPipeline`)**
   * Processes the raw job descriptions.
   * Utilizes regex heuristics to extract specific in-demand technical skills (e.g., Python, SQL, Power BI, AWS, dbt).

3. **Database Integration (`PostgresPipeline`)**
   * Connects securely to a Supabase PostgreSQL instance via connection pooling.
   * Auto-generates the `job_postings` table and analytical SQL Views.
   * Performs an `UPSERT` using a generated `job_hash` to maintain a rolling window of active jobs, updating `date_last_seen` to track hiring velocity.

---

## 🥷 Stealth & Anti-Bot Measures

To ensure reliable data collection, the pipeline employs several measures:

1. **Rotating User-Agents:** Draws from a curated pool of realistic browser strings.
2. **Dynamic Headers:** Mimics human browser behavior with appropriate headers.
3. **Randomized Delays:** Implements delays between requests to avoid rate limiting.
4. **Exponential Backoff:** Gracefully handles server errors and rate limits.

---

## 📂 Project Structure

```text
Egypt-Job-Market-Pipeline/
├── .github/workflows/
│   └── scraper.yml              # CI/CD pipeline for automated scraping
├── bi_jobs/
│   ├── spiders/
│   │   └── wuzzuf_spider.py     # Main Wuzzuf spider
│   ├── items.py                 # Scrapy Item definition
│   ├── middlewares.py           # Stealth and rotation middlewares
│   ├── pipelines.py             # NLP extraction & PostgreSQL integration
│   └── settings.py              # Scrapy configuration
└── requirements.txt             # Python dependencies
```

---

## 🚀 Setup & Execution

### Prerequisites
* Python 3.10+
* Playwright browsers
* A PostgreSQL Database URL (configured as `DATABASE_URL` environment variable)

### Installation
```bash
pip install -r requirements.txt
playwright install chromium
playwright install-deps
```

### Running Locally
```bash
# Ensure your DATABASE_URL is exported in your terminal
export DATABASE_URL="your_connection_string"
scrapy crawl wuzzuf_spider
```

---

## ⚙️ Automation (GitHub Actions)

The pipeline is fully automated via GitHub Actions (`scraper.yml`):
* **Schedule:** Runs automatically every 2 days.
* **Database Sync:** Connects to the cloud PostgreSQL database and upserts the latest job market state, ensuring the Power BI dashboard is always live.
