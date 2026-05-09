"""
normalize.py — Bronze → Silver Normalization Layer
====================================================
Reads raw job_postings (Bronze), applies NLP normalization,
and upserts into job_postings_clean (Silver).

Can be run standalone:
    python normalize.py

Or triggered automatically after each crawl via NormalizationExtension.
"""

import os
import re
import logging
import hashlib
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
#  JOB FAMILY TAXONOMY — Egyptian Market
# ══════════════════════════════════════════════════════════════
JOB_FAMILY_MAP = [
    ("Data Analytics & BI",   [
        "data analyst", "bi analyst", "business analyst", "reporting analyst",
        "insights analyst", "analytics manager", "analytics engineer",
    ]),
    ("BI Development",        [
        "power bi", "tableau", "qlik", "looker", "bi developer",
        "data visualization", "bi consultant",
    ]),
    ("Data Engineering",      [
        "data engineer", "etl developer", "etl engineer", "data pipeline",
        "data warehouse", "dbt", "spark engineer", "airflow", "data architect",
    ]),
    ("Data Science & ML",     [
        "data scientist", "machine learning", "ml engineer", "ai engineer",
        "deep learning", "nlp engineer", "computer vision", "research scientist",
    ]),
    ("Software Engineering",  [
        "software engineer", "backend developer", "backend engineer",
        ".net developer", "java developer", "python developer",
        "c++ developer", "node developer", "api developer", "systems engineer",
    ]),
    ("Frontend & Web Dev",    [
        "frontend developer", "frontend engineer", "web developer",
        "react developer", "angular developer", "vue developer",
        "ui developer", "full stack", "fullstack",
    ]),
    ("Mobile Development",    [
        "mobile developer", "android developer", "ios developer",
        "flutter developer", "react native", "mobile engineer",
    ]),
    ("DevOps & Cloud",        [
        "devops", "cloud engineer", "cloud architect", "aws engineer",
        "azure engineer", "kubernetes", "site reliability", "sre",
        "infrastructure engineer", "platform engineer",
    ]),
    ("IT & Systems",          [
        "it support", "system administrator", "network engineer",
        "helpdesk", "it engineer", "infrastructure", "system analyst",
        "technical support", "it specialist",
    ]),
    ("Product Management",    [
        "product manager", "product owner", "scrum master",
        "product lead", "head of product",
    ]),
    ("Project Management",    [
        "project manager", "pmo", "program manager", "delivery manager",
        "project coordinator", "project lead",
    ]),
    ("Finance & Accounting",  [
        "accountant", "financial analyst", "finance manager", "treasury",
        "auditor", "tax specialist", "cost accountant", "cfo",
        "financial controller", "accounts payable", "accounts receivable",
    ]),
    ("Sales & BD",            [
        "sales representative", "sales manager", "account manager",
        "business development", "key account", "sales executive",
        "sales engineer", "b2b sales", "commercial manager",
    ]),
    ("Marketing & Digital",   [
        "marketing manager", "digital marketing", "seo specialist",
        "social media", "content writer", "content creator",
        "growth hacker", "brand manager", "marketing analyst", "media buyer",
    ]),
    ("Human Resources",       [
        "hr specialist", "hr manager", "human resources", "talent acquisition",
        "recruiter", "payroll", "hr business partner", "learning and development",
        "compensation", "organizational development",
    ]),
    ("Customer Service",      [
        "customer service", "customer support", "call center", "cx specialist",
        "customer success", "customer experience", "contact center",
    ]),
    ("Operations & SCM",      [
        "operations manager", "supply chain", "logistics", "procurement",
        "purchasing", "warehouse", "inventory", "fleet", "operations analyst",
    ]),
    ("Quality Assurance",     [
        "qa engineer", "quality assurance", "test engineer", "software tester",
        "qa analyst", "quality control", "testing specialist",
    ]),
    ("Architecture & Design", [
        "solutions architect", "enterprise architect", "ui/ux", "ux designer",
        "graphic designer", "product designer", "visual designer",
        "ui designer", "brand designer",
    ]),
    ("Legal & Compliance",    [
        "lawyer", "legal counsel", "compliance officer", "legal advisor",
        "contract manager", "regulatory",
    ]),
]

# ══════════════════════════════════════════════════════════════
#  SENIORITY PATTERNS (5-tier, applied to title + description)
# ══════════════════════════════════════════════════════════════
SENIORITY_PATTERNS = {
    "Executive": [
        r"\b(chief|cto|ceo|cfo|coo|vp|vice president|director|head of|managing director)\b",
    ],
    "Senior": [
        r"\b(senior|sr\.?|lead|principal|staff|specialist\s+\w+|expert)\b",
    ],
    "Intern": [
        r"\b(intern|internship|trainee|graduate\s+trainee|co-?op)\b",
    ],
    "Entry": [
        r"\b(junior|jr\.?|fresh\s*graduate|entry.level|associate|beginner|0[- ]?1\s*year)\b",
    ],
    # "Mid" is the default — no explicit pattern needed
}

# ══════════════════════════════════════════════════════════════
#  TITLE NOISE PATTERNS (strip before classification)
# ══════════════════════════════════════════════════════════════
TITLE_NOISE = [
    r"\s*[-–|·•]\s*.+$",                      # anything after separator
    r"\b(urgent(ly)?|asap|immediately)\b",
    r"\b(cairo|giza|alexandria|remote|hybrid|onsite|on-site)\b",
    r"\b(branch|office|egypt|egypt-based)\b",
    r"\b(needed|required|wanted|vacancy|vacancies|hiring|open)\b",
    r"\b(full.?time|part.?time|freelance|contract|internship)\b",
    r"\(.*?\)",                                # anything in parentheses
    r"\[.*?\]",                                # anything in brackets
    r"\s{2,}",                                 # extra spaces
]

# ══════════════════════════════════════════════════════════════
#  LOCATION → GOVERNORATE MAPPING
# ══════════════════════════════════════════════════════════════
GOVERNORATE_MAP = {
    # Cairo
    "cairo": "Cairo", "new cairo": "Cairo", "nasr city": "Cairo",
    "heliopolis": "Cairo", "maadi": "Cairo", "zamalek": "Cairo",
    "downtown cairo": "Cairo", "fifth settlement": "Cairo",
    "15th of may": "Cairo", "sheraton": "Cairo", "tagamo3": "Cairo",
    "new administrative capital": "Cairo", "shorouk": "Cairo",
    "katameya": "Cairo", "rehab": "Cairo",
    # Giza
    "giza": "Giza", "6th of october": "Giza", "6th october": "Giza",
    "sheikh zayed": "Giza", "mohandessin": "Giza", "dokki": "Giza",
    "haram": "Giza", "faisal": "Giza", "hadayek al ahram": "Giza",
    # Alexandria
    "alexandria": "Alexandria", "alex": "Alexandria",
    "borg el arab": "Alexandria", "smouha": "Alexandria",
    "miami": "Alexandria", "sidi gaber": "Alexandria",
    # Other Governorates
    "mansoura": "Dakahlia", "tanta": "Gharbia", "zagazig": "Sharqia",
    "assiut": "Assiut", "aswan": "Aswan", "luxor": "Luxor",
    "ismailia": "Ismailia", "suez": "Suez", "port said": "Port Said",
    "portsaid": "Port Said", "damietta": "Damietta", "fayoum": "Faiyum",
    "beni suef": "Beni Suef", "minya": "Minya", "sohag": "Sohag",
    "qena": "Qena", "hurghada": "Red Sea", "sharm el sheikh": "South Sinai",
    "ain sokhna": "Suez",
}

# ══════════════════════════════════════════════════════════════
#  SKILLS LIST (reused from SkillExtractionPipeline)
# ══════════════════════════════════════════════════════════════
SKILLS_DICT = {
    "Python": r"\bpython\b",
    "SQL": r"\bsql\b",
    "Java": r"\bjava\b(?!script)",
    "JavaScript": r"\bjavascript\b|\bjs\b",
    "TypeScript": r"\btypescript\b",
    "C#": r"\bc#\b|\.net",
    "C++": r"\bc\+\+\b",
    "PHP": r"\bphp\b",
    "R": r"\br programming\b|\br studio\b|\brstudio\b",
    "Swift": r"\bswift\b",
    "Kotlin": r"\bkotlin\b",
    "Power BI": r"\bpower\s*bi\b",
    "Tableau": r"\btableau\b",
    "Excel": r"\bexcel\b",
    "Looker": r"\blooker\b",
    "DAX": r"\bdax\b",
    "SSIS": r"\bssis\b",
    "SSRS": r"\bssrs\b",
    "SSAS": r"\bssas\b",
    "Spark": r"\bspark\b|\bpyspark\b",
    "Kafka": r"\bkafka\b",
    "Airflow": r"\bairflow\b",
    "dbt": r"\bdbt\b|data build tool",
    "ETL": r"\betl\b",
    "Snowflake": r"\bsnowflake\b",
    "Databricks": r"\bdatabricks\b",
    "Hadoop": r"\bhadoop\b",
    "AWS": r"\baws\b|\bamazon web services\b",
    "Azure": r"\bazure\b",
    "GCP": r"\bgcp\b|\bgoogle cloud\b",
    "Docker": r"\bdocker\b",
    "Kubernetes": r"\bkubernetes\b|\bk8s\b",
    "Terraform": r"\bterraform\b",
    "Git": r"\bgit\b|\bgithub\b|\bgitlab\b",
    "Linux": r"\blinux\b|\bubuntu\b",
    "PostgreSQL": r"\bpostgres\b|\bpostgresql\b",
    "MySQL": r"\bmysql\b",
    "MongoDB": r"\bmongodb\b|\bmongo\b",
    "Redis": r"\bredis\b",
    "Oracle": r"\boracle\b",
    "NoSQL": r"\bnosql\b|\bcassandra\b|\bdynamodb\b",
    "React": r"\breact\b|\breactjs\b",
    "Angular": r"\bangular\b",
    "Vue.js": r"\bvue\b|\bvuejs\b",
    "Node.js": r"\bnode\.?js\b",
    "Django": r"\bdjango\b",
    "Flask": r"\bflask\b",
    "Spring": r"\bspring\s*boot\b|\bspring\b",
    "Machine Learning": r"\bmachine learning\b",
    "Deep Learning": r"\bdeep learning\b|\bneural network\b",
    "TensorFlow": r"\btensorflow\b",
    "PyTorch": r"\bpytorch\b",
    "NLP": r"\bnlp\b|\bnatural language\b",
    "Agile": r"\bagile\b|\bscrum\b|\bkanban\b",
    "Jira": r"\bjira\b",
    "SAP": r"\bsap\b",
    "Figma": r"\bfigma\b",
    "Flutter": r"\bflutter\b",
}

# ══════════════════════════════════════════════════════════════
#  NORMALIZATION FUNCTIONS
# ══════════════════════════════════════════════════════════════

def clean_title(title: str) -> str:
    """Strip noise from a raw job title."""
    if not title:
        return ""
    result = title
    for pattern in TITLE_NOISE:
        result = re.sub(pattern, " ", result, flags=re.IGNORECASE)
    return re.sub(r"\s{2,}", " ", result).strip()


def classify_job_family(title_clean: str) -> str:
    """Map a cleaned title to a canonical job family."""
    t = title_clean.lower()
    for family, keywords in JOB_FAMILY_MAP:
        for kw in keywords:
            if kw in t:
                return family
    return "Other"


def extract_seniority(title: str, description: str = "") -> str:
    """Extract 5-tier seniority from title first, then description."""
    text = (title or "").lower()
    for tier, patterns in SENIORITY_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, text, re.IGNORECASE):
                return tier
    # Fall back to description if title has no seniority signal
    desc_text = (description or "")[:500].lower()
    for tier, patterns in SENIORITY_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, desc_text, re.IGNORECASE):
                return tier
    return "Mid"


def parse_location(raw_location: str):
    """Return (city, governorate) from a raw location string."""
    if not raw_location or raw_location.strip().lower() in ("egypt", "not specified", ""):
        return "Not Specified", "Not Specified"

    loc_lower = raw_location.lower()

    # Try longest match first
    for keyword in sorted(GOVERNORATE_MAP.keys(), key=len, reverse=True):
        if keyword in loc_lower:
            # City = the keyword (title-cased), governorate from map
            city = keyword.title()
            gov = GOVERNORATE_MAP[keyword]
            return city, gov

    # Fallback: use first part before comma as city, Egypt as governorate
    parts = [p.strip() for p in raw_location.split(",")]
    city = parts[0] if parts else raw_location
    return city, "Egypt"


def extract_experience_years(experience_str: str, description: str = ""):
    """Extract (min, max) years of experience as integers."""
    text = experience_str or description or ""
    text = text[:500]

    # "0 - 1 Year" or "3 - 5 Years"
    m = re.search(r"(\d+)\s*[-–to]+\s*(\d+)\s*years?", text, re.IGNORECASE)
    if m:
        return int(m.group(1)), int(m.group(2))

    # "5+ Years" or "5 or more"
    m = re.search(r"(\d+)\+?\s*(?:or more\s*)?years?", text, re.IGNORECASE)
    if m:
        return int(m.group(1)), None

    # Fresh graduate
    if re.search(r"fresh.?grad|no\s+experience|0\s*years?", text, re.IGNORECASE):
        return 0, 1

    return None, None


def extract_education(description: str) -> str:
    """Extract education level from description text."""
    if not description:
        return "Not Specified"
    desc = description.lower()
    if re.search(r"\bphd\b|\bdoctorate\b", desc):
        return "PhD"
    if re.search(r"\bmaster['\u2019s]*s?\b|\bmsc\b|\bmba\b", desc):
        return "Master's"
    if re.search(r"\bbachelor['\u2019s]*s?\b|\bbsc\b|\bb\.?sc\b|\buniversity degree\b|\bba\b|\bb\.?a\b", desc):
        return "Bachelor's"
    if re.search(r"\bdiploma\b|\btechnical\b|\bvocational\b", desc):
        return "Diploma"
    return "Not Specified"


def extract_salary_from_description(description: str, existing_salary: str = ""):
    """Parse salary numbers from description if raw salary is 'Not Specified'."""
    if existing_salary and existing_salary.lower() not in ("not specified", "", "none"):
        # Try to parse the existing salary field
        m = re.search(r"(\d[\d,]*)\s*[-–]\s*(\d[\d,]*)", existing_salary)
        if m:
            lo = int(m.group(1).replace(",", ""))
            hi = int(m.group(2).replace(",", ""))
            return lo, hi

    if not description:
        return None, None

    desc = description[:1000]

    # "15,000 - 20,000 EGP" or "EGP 15000"
    m = re.search(
        r"(?:egp|le|pounds?)?\s*(\d{4,}(?:[,\s]\d{3})*)\s*[-–to]+\s*(\d{4,}(?:[,\s]\d{3})*)\s*(?:egp|le|pounds?)?",
        desc, re.IGNORECASE
    )
    if m:
        lo = int(re.sub(r"[,\s]", "", m.group(1)))
        hi = int(re.sub(r"[,\s]", "", m.group(2)))
        if lo > 500 and hi > 500:  # sanity check (not years/percentages)
            return lo, hi

    # "up to 25k" or "up to 25,000"
    m = re.search(r"up\s+to\s+(\d+)k?\b", desc, re.IGNORECASE)
    if m:
        val = int(m.group(1))
        if "k" in m.group(0).lower() or val < 1000:
            val *= 1000
        return None, val

    return None, None


def extract_inferred_skills(description: str, existing_skills: list) -> list:
    """Find skills in description not already in the existing skills list."""
    if not description:
        return []
    desc_lower = description.lower()
    existing_lower = {s.lower() for s in (existing_skills or [])}
    found = []
    for skill, pattern in SKILLS_DICT.items():
        if skill.lower() not in existing_lower:
            if re.search(pattern, desc_lower, re.IGNORECASE):
                found.append(skill)
    return found


def normalize_company(name: str) -> str:
    """Basic company name normalization."""
    if not name:
        return ""
    # Strip trailing/leading punctuation, normalize spaces
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"[.,;]+$", "", name)
    return name.title()


def make_clean_hash(source_hash: str) -> str:
    return hashlib.md5(f"clean_{source_hash}".encode()).hexdigest()


# ══════════════════════════════════════════════════════════════
#  MAIN NORMALIZATION RUNNER
# ══════════════════════════════════════════════════════════════

def run_normalization(conn=None, batch_size: int = 500):
    """
    Pull unprocessed rows from job_postings (Bronze),
    normalize them, and upsert into job_postings_clean (Silver).
    """
    close_conn = False
    if conn is None:
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL not set in environment")
        conn = psycopg2.connect(db_url)
        close_conn = True

    try:
        cur = conn.cursor()

        # Fetch rows from Bronze not yet in Silver, or updated after last sync
        cur.execute("""
            SELECT
                jp.job_hash, jp.title, jp.company, jp.location,
                jp.experience, jp.job_type, jp.salary, jp.career_level,
                jp.date_posted, jp.url, jp.description,
                jp.date_first_seen, jp.date_last_seen,
                jp.historical_date, jp.status, jp.skills
            FROM public.job_postings jp
            WHERE jp.status = 'active'
            AND (
                jp.job_hash NOT IN (SELECT source_hash FROM public.job_postings_clean)
                OR jp.date_last_seen > (
                    SELECT COALESCE(MAX(updated_at), '1970-01-01')
                    FROM public.job_postings_clean
                    WHERE source_hash = jp.job_hash
                )
            )
            LIMIT %s
        """, (batch_size,))

        rows = cur.fetchall()
        logger.info(f"[Normalize] Processing {len(rows)} rows from Bronze...")

        upserted = 0
        for row in rows:
            (
                source_hash, title, company, location, experience,
                job_type, salary, career_level, date_posted, url, description,
                date_first_seen, date_last_seen, historical_date, status,
                skills
            ) = row

            # ── Title ──────────────────────────────────────────────
            title_clean = clean_title(title or "")
            job_family = classify_job_family(title_clean)

            # ── Seniority ──────────────────────────────────────────
            seniority_level = extract_seniority(title or "", description or "")

            # ── Career level (backfill from seniority if null) ──────
            if not career_level or career_level.strip().lower() in ("not specified", ""):
                tier_to_level = {
                    "Intern": "Intern",
                    "Entry": "Entry Level",
                    "Mid": "Experienced",
                    "Senior": "Senior",
                    "Executive": "Manager / Director",
                }
                career_level = tier_to_level.get(seniority_level, "Not Specified")

            # ── Location ───────────────────────────────────────────
            city, governorate = parse_location(location or "")

            # ── Experience years ───────────────────────────────────
            years_min, years_max = extract_experience_years(experience or "", description or "")

            # ── Education ──────────────────────────────────────────
            education_level = extract_education(description or "")

            # ── Salary ─────────────────────────────────────────────
            salary_min, salary_max = extract_salary_from_description(description or "", salary or "")

            # ── Inferred skills from description ───────────────────
            inferred_skills = extract_inferred_skills(description or "", skills or [])

            # ── Company name ───────────────────────────────────────
            company_clean = normalize_company(company or "")

            clean_hash = make_clean_hash(source_hash)

            cur.execute("""
                INSERT INTO public.job_postings_clean (
                    clean_hash, source_hash,
                    title, company, location, experience, job_type,
                    salary, career_level, date_posted, url, description,
                    historical_date, status,
                    title_clean, job_family, seniority_level,
                    city, governorate,
                    years_exp_min, years_exp_max,
                    education_level, salary_min, salary_max,
                    skills, inferred_skills,
                    normalized_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, NOW(), NOW()
                )
                ON CONFLICT (clean_hash) DO UPDATE SET
                    title           = EXCLUDED.title,
                    company         = EXCLUDED.company,
                    location        = EXCLUDED.location,
                    job_type        = EXCLUDED.job_type,
                    salary          = EXCLUDED.salary,
                    career_level    = EXCLUDED.career_level,
                    status          = EXCLUDED.status,
                    title_clean     = EXCLUDED.title_clean,
                    job_family      = EXCLUDED.job_family,
                    seniority_level = EXCLUDED.seniority_level,
                    city            = EXCLUDED.city,
                    governorate     = EXCLUDED.governorate,
                    years_exp_min   = EXCLUDED.years_exp_min,
                    years_exp_max   = EXCLUDED.years_exp_max,
                    education_level = EXCLUDED.education_level,
                    salary_min      = EXCLUDED.salary_min,
                    salary_max      = EXCLUDED.salary_max,
                    skills          = EXCLUDED.skills,
                    inferred_skills = EXCLUDED.inferred_skills,
                    updated_at      = NOW()
            """, (
                clean_hash, source_hash,
                title, company_clean, location, experience, job_type,
                salary, career_level, date_posted, url, description,
                historical_date, status,
                title_clean, job_family, seniority_level,
                city, governorate,
                years_min, years_max,
                education_level, salary_min, salary_max,
                skills or [], inferred_skills,
            ))
            upserted += 1

        conn.commit()
        logger.info(f"[Normalize] Done. Upserted {upserted} rows into job_postings_clean.")
        return upserted

    except Exception as e:
        conn.rollback()
        logger.error(f"[Normalize] Error: {e}")
        raise
    finally:
        if close_conn:
            conn.close()


# ── Standalone entry point ────────────────────────────────────
if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    load_dotenv()
    logger.info("Starting standalone normalization run...")
    n = run_normalization()
    logger.info(f"Normalization complete. {n} rows processed.")
