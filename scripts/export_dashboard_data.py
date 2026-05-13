"""
export_dashboard_data.py
Pulls real aggregates from Supabase and prints them as JSON
for embedding into the HTML dashboard and infographic.
"""
import os, json, psycopg2
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))
DB_URL = os.environ['DATABASE_URL']

conn = psycopg2.connect(DB_URL)
cur  = conn.cursor()

def q(sql):
    cur.execute(sql)
    return cur.fetchall()

# ── KPIs ──────────────────────────────────────────────────────────────────────
cur.execute("SELECT COUNT(*) FROM job_postings;")
total = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM job_postings WHERE status='active';")
active = cur.fetchone()[0]

cur.execute("SELECT COUNT(DISTINCT company) FROM job_postings WHERE status='active';")
companies = cur.fetchone()[0]

cur.execute("SELECT MIN(date_first_seen), MAX(date_first_seen) FROM job_postings;")
date_range = cur.fetchone()

# ── Top skill ─────────────────────────────────────────────────────────────────
cur.execute("""
    SELECT skill, COUNT(*) as cnt
    FROM v_skill_frequency
    GROUP BY skill ORDER BY cnt DESC LIMIT 1;
""")
top_skill_row = cur.fetchone()
top_skill = top_skill_row[0] if top_skill_row else 'N/A'

# ── Top city (from silver if available, else bronze) ──────────────────────────
try:
    cur.execute("""
        SELECT city, COUNT(*) as cnt FROM job_postings_clean
        WHERE status='active' AND city IS NOT NULL AND city != ''
        GROUP BY city ORDER BY cnt DESC LIMIT 1;
    """)
    top_city_row = cur.fetchone()
    top_city = top_city_row[0] if top_city_row else 'Cairo'
except:
    top_city = 'Cairo'
conn.rollback()

# ── Skills top 15 ─────────────────────────────────────────────────────────────
rows = q("""
    SELECT skill, COUNT(*) as cnt FROM v_skill_frequency
    GROUP BY skill ORDER BY cnt DESC LIMIT 15;
""")
skills15 = [{'skill': r[0], 'count': r[1]} for r in rows]

# ── Hiring trend (last 60 days) ───────────────────────────────────────────────
rows = q("""
    SELECT scrape_date::text, new_jobs FROM v_hiring_trends
    ORDER BY scrape_date DESC LIMIT 60;
""")
trend = [{'date': str(r[0]), 'count': r[1]} for r in reversed(rows)]

# ── Top 10 companies ──────────────────────────────────────────────────────────
rows = q("""
    SELECT company, open_positions FROM v_top_companies LIMIT 10;
""")
companies_top = [{'company': r[0], 'count': r[1]} for r in rows]

# ── Job type breakdown ────────────────────────────────────────────────────────
rows = q("""
    SELECT job_type_individual, COUNT(*) as cnt
    FROM v_job_types_expanded
    GROUP BY job_type_individual ORDER BY cnt DESC LIMIT 8;
""")
job_types = [{'type': r[0], 'count': r[1]} for r in rows]

# ── Career level ──────────────────────────────────────────────────────────────
rows = q("""
    SELECT career_level, count FROM v_career_level_distribution
    WHERE career_level IS NOT NULL AND career_level != '' LIMIT 8;
""")
career_levels = [{'level': r[0], 'count': r[1]} for r in rows]

# ── Location ──────────────────────────────────────────────────────────────────
try:
    rows = q("""
        SELECT governorate, COUNT(*) as cnt FROM job_postings_clean
        WHERE status='active' AND governorate IS NOT NULL AND governorate != ''
        GROUP BY governorate ORDER BY cnt DESC LIMIT 8;
    """)
    geo = [{'name': r[0], 'count': r[1]} for r in rows]
except:
    conn.rollback()
    rows = q("""
        SELECT location, count FROM v_location_distribution LIMIT 8;
    """)
    geo = [{'name': r[0], 'count': r[1]} for r in rows]

# ── Seniority (silver) ────────────────────────────────────────────────────────
try:
    rows = q("""
        SELECT seniority_level, COUNT(*) as cnt FROM job_postings_clean
        WHERE status='active' AND seniority_level IS NOT NULL AND seniority_level != ''
        GROUP BY seniority_level ORDER BY cnt DESC;
    """)
    seniority = [{'level': r[0], 'count': r[1]} for r in rows]
except:
    conn.rollback()
    seniority = []

# ── Daily by weekday ──────────────────────────────────────────────────────────
rows = q("""
    SELECT TO_CHAR(date_first_seen, 'Dy') as dow,
           EXTRACT(DOW FROM date_first_seen) as dow_num,
           COUNT(*) as cnt
    FROM job_postings
    GROUP BY dow, dow_num ORDER BY dow_num;
""")
weekday = [{'day': r[0], 'count': r[2]} for r in rows]

# ── Monthly ───────────────────────────────────────────────────────────────────
rows = q("""
    SELECT TO_CHAR(date_first_seen, 'Mon YYYY') as month,
           DATE_TRUNC('month', date_first_seen) as m,
           COUNT(*) as cnt
    FROM job_postings
    GROUP BY month, m ORDER BY m;
""")
monthly = [{'month': r[0], 'count': r[2]} for r in rows]

cur.close()
conn.close()

output = {
    'kpis': {
        'total': total,
        'active': active,
        'companies': companies,
        'top_skill': top_skill,
        'top_city': top_city,
        'date_min': str(date_range[0])[:10] if date_range[0] else '',
        'date_max': str(date_range[1])[:10] if date_range[1] else '',
    },
    'skills15': skills15,
    'trend': trend,
    'companies_top': companies_top,
    'job_types': job_types,
    'career_levels': career_levels,
    'geo': geo,
    'seniority': seniority,
    'weekday': weekday,
    'monthly': monthly,
}

print(json.dumps(output, indent=2, default=str))
