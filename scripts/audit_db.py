"""
Full database audit: tables, views, row counts, columns, sample data.
"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import os
import json
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")

QUERIES = {
    "all_tables": """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_type, table_name;
    """,
    "row_counts": """
        SELECT
            schemaname,
            relname AS table_name,
            n_live_tup AS estimated_rows
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
        ORDER BY n_live_tup DESC;
    """,
    "job_postings_columns": """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'job_postings'
        ORDER BY ordinal_position;
    """,
    "job_postings_clean_columns": """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'job_postings_clean'
        ORDER BY ordinal_position;
    """,
    "view_definitions": """
        SELECT viewname, definition
        FROM pg_views
        WHERE schemaname = 'public'
        ORDER BY viewname;
    """,
    "job_postings_stats": """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE status = 'active') AS active_rows,
            COUNT(*) FILTER (WHERE status = 'inactive') AS inactive_rows,
            MIN(historical_date) AS earliest_date,
            MAX(historical_date) AS latest_date,
            COUNT(DISTINCT company) AS unique_companies,
            COUNT(DISTINCT location) AS unique_locations,
            COUNT(DISTINCT career_level) AS unique_career_levels,
            COUNT(DISTINCT job_type) AS unique_job_types
        FROM public.job_postings;
    """,
    "job_postings_clean_stats": """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE status = 'active') AS active_rows,
            COUNT(DISTINCT job_family) AS unique_job_families,
            COUNT(DISTINCT seniority_level) AS unique_seniority_levels,
            COUNT(DISTINCT governorate) AS unique_governorates,
            COUNT(DISTINCT city) AS unique_cities,
            COUNT(*) FILTER (WHERE skills IS NOT NULL) AS rows_with_skills,
            COUNT(*) FILTER (WHERE inferred_skills IS NOT NULL) AS rows_with_inferred_skills,
            COUNT(*) FILTER (WHERE salary_min IS NOT NULL) AS rows_with_salary,
            MIN(historical_date) AS earliest_date,
            MAX(historical_date) AS latest_date
        FROM public.job_postings_clean;
    """,
    "career_level_dist": """
        SELECT career_level, COUNT(*) AS count
        FROM public.job_postings
        WHERE status = 'active'
        GROUP BY career_level
        ORDER BY count DESC;
    """,
    "job_family_dist": """
        SELECT job_family, COUNT(*) AS count
        FROM public.job_postings_clean
        WHERE status = 'active'
        GROUP BY job_family
        ORDER BY count DESC
        LIMIT 20;
    """,
    "seniority_dist": """
        SELECT seniority_level, COUNT(*) AS count
        FROM public.job_postings_clean
        WHERE status = 'active'
        GROUP BY seniority_level
        ORDER BY count DESC;
    """,
    "governorate_dist": """
        SELECT governorate, COUNT(*) AS count
        FROM public.job_postings_clean
        WHERE status = 'active'
        GROUP BY governorate
        ORDER BY count DESC
        LIMIT 15;
    """,
    "top_skills_clean": """
        SELECT skill, COUNT(*) AS job_count
        FROM public.v_clean_skills
        GROUP BY skill
        ORDER BY job_count DESC
        LIMIT 25;
    """,
    "top_skills_bronze": """
        SELECT skill, COUNT(*) AS job_count
        FROM public.v_job_skills_expanded
        GROUP BY skill
        ORDER BY job_count DESC
        LIMIT 25;
    """,
    "job_types_clean": """
        SELECT job_type_individual, COUNT(*) AS count
        FROM public.v_clean_job_types
        GROUP BY job_type_individual
        ORDER BY count DESC;
    """,
    "job_types_bronze": """
        SELECT job_type_individual, COUNT(*) AS count
        FROM public.v_job_types_expanded
        GROUP BY job_type_individual
        ORDER BY count DESC;
    """,
    "location_hierarchy_sample": """
        SELECT *
        FROM public.v_location_hierarchy
        LIMIT 20;
    """,
    "date_distribution": """
        SELECT
            DATE_TRUNC('month', historical_date) AS month,
            COUNT(*) AS job_count
        FROM public.job_postings
        WHERE status = 'active' AND historical_date IS NOT NULL
        GROUP BY 1
        ORDER BY 1;
    """,
    "sample_job_postings_clean": """
        SELECT
            clean_hash, title_clean, job_family, seniority_level,
            governorate, city, years_exp_min, years_exp_max,
            salary_min, salary_max, education_level,
            array_length(skills, 1) AS skill_count,
            array_length(inferred_skills, 1) AS inferred_skill_count,
            historical_date, status
        FROM public.job_postings_clean
        WHERE status = 'active'
        LIMIT 5;
    """,
    "check_extra_views": """
        SELECT viewname FROM pg_views WHERE schemaname = 'public' ORDER BY viewname;
    """,
    "check_all_objects": """
        SELECT
            n.nspname AS schema,
            c.relname AS name,
            CASE c.relkind
                WHEN 'r' THEN 'TABLE'
                WHEN 'v' THEN 'VIEW'
                WHEN 'm' THEN 'MATERIALIZED VIEW'
                WHEN 'f' THEN 'FOREIGN TABLE'
                WHEN 'p' THEN 'PARTITIONED TABLE'
            END AS type
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r','v','m','f','p')
        ORDER BY type, name;
    """,
}


def run_query(conn, label, sql):
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            return {"columns": cols, "rows": [list(r) for r in rows]}
    except Exception as e:
        return {"error": str(e)}


def main():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True

    results = {}
    for label, sql in QUERIES.items():
        print(f"Running: {label}...")
        results[label] = run_query(conn, label, sql)

    conn.close()

    # Save to JSON first
    with open("d:/DS FP/db_audit_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    print("Results saved to db_audit_results.json")

    # Pretty-print
    for label, data in results.items():
        print(f"\n{'='*60}")
        print(f"  {label.upper()}")
        print(f"{'='*60}")
        if "error" in data:
            print(f"  ERROR: {data['error']}")
        else:
            cols = data["columns"]
            print("  Columns:", cols)
            for row in data["rows"]:
                row_dict = dict(zip(cols, row))
                try:
                    print(" ", row_dict)
                except Exception:
                    print("  [row with unencodable characters skipped]")




if __name__ == "__main__":
    main()
