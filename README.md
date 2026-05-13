# 📊 Egypt Job Market Intelligence Pipeline

> An automated, end-to-end data engineering pipeline that harvests job postings from **multiple Egyptian job posting websites**, processes them through a **Medallion Data Warehouse**, and powers a live **Power BI dashboard** with real-time market intelligence.

---

## 🧰 Tech Stack

| Layer | Technology |
|---|---|
| **Web Scraping** | Scrapy + Playwright (headless browser) |
| **NLP & Skill Extraction** | Rule-based Regex NLP (60+ skills) |
| **Cloud Database** | Supabase (PostgreSQL) |
| **Data Warehouse** | Bronze → Silver → Gold (Medallion Architecture) |
| **Normalization** | Custom Python NLP (`normalize.py`) |
| **Visualization** | Power BI (Star Schema, DAX measures) |
| **Automation** | GitHub Actions (scheduled every 2 days) |

---

## 🏗️ Architecture & Data Flow

The pipeline follows a **Medallion Architecture**, moving data from raw extraction through progressive refinement into analytics-ready layers.

```
[Job Postings Websites]
      │  Scrapy + Playwright crawl
      ▼
┌─────────────────────────────────────────┐
│  BRONZE LAYER  —  job_postings          │  Raw scraped data
│  Auto-created by spider on first run    │  job_hash, date_first_seen,
│  UPSERT via job_hash deduplication      │  date_last_seen, status
└─────────────────────────────────────────┘
      │  normalize.py (auto-triggered after crawl)
      ▼
┌─────────────────────────────────────────┐
│  SILVER LAYER  —  job_postings_clean    │  Normalized & enriched
│  NLP classifies job family, seniority   │  job_family, seniority_level,
│  Extracts city/governorate, salary,     │  city, governorate,
│  education, and inferred skills         │  salary_min/max, education
└─────────────────────────────────────────┘
      │  Analytical SQL Views
      ▼
┌─────────────────────────────────────────┐
│  GOLD LAYER  —  Star Schema Views       │  Power BI consumption
│  Fact table + 8 dimension/bridge views  │  DAX measures, KPIs,
│  Live connection from Power BI          │  interactive dashboards
└─────────────────────────────────────────┘
```

---

## ⚙️ Pipeline Stages

The Scrapy pipeline runs 5 sequential processing stages on every scraped item:

| Priority | Class | Responsibility |
|---|---|---|
| `100` | `CleaningPipeline` | Whitespace normalization, title scrubbing, non-job filtering, historical date parsing |
| `200` | `DuplicateFilterPipeline` | Drops duplicate URLs within the same crawl session |
| `250` | `CsvBackupPipeline` | Writes all items to a local dated CSV (`output/bi_jobs_YYYY-MM-DD.csv`) as a safety net |
| `260` | `SkillExtractionPipeline` | Regex NLP skill tagging across 60+ technologies |
| `300` | `PostgresPipeline` | UPSERT into Supabase, auto-creates tables and all analytical views |

---

## 🥷 Stealth & Anti-Bot Measures

To ensure reliable, uninterrupted data collection, the pipeline employs:

1. **Rotating User-Agents** — Draws from a curated pool of realistic browser strings.
2. **Dynamic Headers** — Mimics human browser behavior with appropriate request headers.
3. **Randomized Delays** — Implements variable delays between requests to avoid rate limiting.
4. **Exponential Backoff** — Gracefully handles server errors and transient rate limits.

---

## 🗄️ Data Warehouse

### Bronze Layer — `job_postings`

The raw landing zone. Auto-provisioned by the spider on the first run with no manual setup required.

**Key fields:** `job_hash` (deduplication key), `title`, `company`, `location`, `experience`, `job_type`, `career_level`, `skills[]`, `date_first_seen`, `date_last_seen`, `status` (`active` / `expired`)

### Silver Layer — `job_postings_clean`

The normalized, analytics-ready layer. Provisioned via `supabase_setup/create_silver_table.sql` and populated by `normalize.py`.

**Enriched fields added by NLP:**

| Field | Description |
|---|---|
| `title_clean` | De-noised job title (strips location/type suffixes) |
| `job_family` | Canonical role category (20+ families, e.g. *Data Engineering*, *BI Development*, *DevOps & Cloud*) |
| `seniority_level` | 5-tier classification: `Intern` / `Entry` / `Mid` / `Senior` / `Executive` |
| `city` / `governorate` | Parsed from raw location string (40+ Egyptian city mappings) |
| `years_exp_min` / `years_exp_max` | Parsed from experience field and description text |
| `education_level` | Extracted from description (`Bachelor's`, `Master's`, `PhD`, `Diploma`) |
| `salary_min` / `salary_max` | Parsed EGP salary ranges from description free-text |
| `inferred_skills` | Skills found in description not captured by the spider's initial extraction |

### Gold Layer — Analytical Views

Views consumed directly by Power BI via a live PostgreSQL connection:

**Bronze Views (raw analytics):**

| View | Purpose |
|---|---|
| `v_skill_frequency` | Skill demand ranking by job count |
| `v_hiring_trends` | New job postings per day (time-series) |
| `v_top_companies` | Companies ranked by open positions |
| `v_job_type_breakdown` | On-site / Hybrid / Remote distribution |
| `v_career_level_distribution` | Seniority distribution across active jobs |
| `v_location_distribution` | Job count by raw location string |
| `v_location_hierarchy` | Parsed country / city / area hierarchy |
| `v_job_types_expanded` | Unnested job types (bridge table for star schema) |
| `v_job_skills_expanded` | Unnested skills (bridge table for star schema) |

**Silver Views (normalized analytics):**

| View | Purpose |
|---|---|
| `v_clean_skills` | Deduplicated skills (original + inferred) per job, with `job_family` and `seniority_level` |
| `v_clean_job_types` | Unnested job types from the Silver layer |

---

## 📊 Power BI Dashboard

**File:** `powerbi/Egyptian_Job_Market_Dashboard.pbix`

The dashboard connects directly to Supabase via a live PostgreSQL connection and is built on a **Star Schema** using the Silver layer as the fact table, with the analytical views as dimension/bridge tables.

**Themes:**
- `powerbi/JobMarket_theme.json` — Brand-aligned color scheme
- `EgyptJobMarket_CLevel_Theme.json` — Clean, C-level executive theme (light, professional)

**Dashboard Pages:**

| Page | Key Visuals |
|---|---|
| **Overview** | Total jobs KPIs, trend line, top skills bar chart |
| **Skills Demand** | Skills frequency ranking, skill-by-job-family heatmap |
| **Hiring Trends** | Time-series of new postings, scrape cadence |
| **Company Intelligence** | Top hiring companies, company-level skill breakdown |
| **Geographic Distribution** | Jobs by governorate map and bar chart |
| **Job Type & Seniority** | On-site/Hybrid/Remote donut, seniority funnel |

---

## 📂 Project Structure

```text
Egypt-Job-Market-Pipeline/
├── .github/workflows/
│   └── scraper.yml                       # CI/CD: scheduled spider run every 2 days
├── bi_jobs/                              # Scrapy project root
│   ├── spiders/
│   │   └── web_spider.py                 # Main crawler (Scrapy + Playwright)
│   ├── items.py                          # Scrapy Item schema definition
│   ├── middlewares.py                    # Stealth: user-agent rotation, delays, backoff
│   ├── normalize.py                      # Bronze → Silver NLP normalization engine
│   ├── pipelines.py                      # 5-stage processing pipeline
│   ├── extensions.py                     # Custom Scrapy extensions (auto-triggers normalization)
│   └── settings.py                       # Scrapy configuration & pipeline ordering
├── notebooks/
│   └── analysis.ipynb                    # Exploratory data analysis
├── output/                               # Local CSV backups — one file per run date (gitignored)
├── powerbi/
│   ├── Egyptian_Job_Market_Dashboard.pbix
│   └── JobMarket_theme.json              # Power BI color theme
├── scripts/
│   └── audit_db.py                       # Database audit, health checks & row counts
├── supabase_setup/
│   └── create_silver_table.sql           # Silver layer DDL + analytical views
├── EgyptJobMarket_CLevel_Theme.json      # C-Level executive Power BI theme
├── requirements.txt                      # Python dependencies
├── scrapy.cfg                            # Scrapy project config
└── .gitignore
```

---

## 🚀 Setup & Execution

### Prerequisites

- Python 3.10+
- A Supabase project with a **PostgreSQL connection string**
- Playwright (for JavaScript-rendered pages)

### 1. Install Dependencies

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure Environment

```bash
# Create a .env file or export the variable in your terminal
export DATABASE_URL="postgresql://user:password@host:port/dbname"
```

### 3. Initialize the Silver Layer (first-time only)

Run `supabase_setup/create_silver_table.sql` in your Supabase SQL Editor to create the `job_postings_clean` table and its analytical views.

### 4. Run the Spider

```bash
scrapy crawl web_spider
```

The spider will:
1. Crawl job posting websites for target job roles
2. Process all items through the 5-stage pipeline
3. UPSERT results into `job_postings` (Bronze)
4. Automatically trigger `normalize.py` to populate `job_postings_clean` (Silver)

### 5. Run Normalization Standalone (optional)

```bash
python bi_jobs/normalize.py
```

---

## ⚙️ Automation (GitHub Actions)

The pipeline is fully automated via GitHub Actions (`.github/workflows/scraper.yml`):

- **Schedule:** Runs automatically every **2 days**
- **Secrets Required:** `DATABASE_URL` must be set as a GitHub repository secret
- **On each run:**
  - Crawls job posting websites for all target roles
  - UPSERTs new/updated jobs into the Bronze layer
  - Marks stale jobs as `expired` (status lifecycle management)
  - Triggers Silver layer normalization
  - Power BI dashboard reflects the updated data on next refresh

---

## 🔭 Future Work

- **Multi-source expansion** — Add scrapers for additional Egyptian and regional job boards (e.g., LinkedIn Egypt, Forasna, Bayt), with a `source` field added to the Bronze layer to enable cross-platform comparisons and unified market analytics.
- **Gemini API NLP** — Replace the current regex-based classification engine with Gemini API calls for richer, context-aware job title normalization, seniority inference, and Arabic/English skill extraction.
- **Real-time streaming** — Move from scheduled batch scraping to event-driven ingestion using a message queue (e.g., Kafka or Supabase Realtime) for continuous, low-latency data updates.
- **Salary benchmarking** — Enrich parsed salary data with inflation adjustments and cross-company / cross-sector benchmarks to produce actionable compensation intelligence.
- **Interactive web dashboard** — Publish a standalone HTML dashboard and infographic derived from the Gold layer for stakeholders who do not have access to Power BI.
