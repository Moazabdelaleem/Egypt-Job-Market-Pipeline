"""
Egypt Job Market Analysis
=========================
Loads data from the Supabase PostgreSQL database, performs exploratory
data analysis, and saves a local CSV snapshot for offline monitoring.

Usage:
    python analysis.py

Requires DATABASE_URL environment variable to be set, or a .env file.
"""

import os
import sys
import pandas as pd
import matplotlib
matplotlib.use('Agg')          # non-interactive backend for CI
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from datetime import datetime

# ── Try to load .env for local development ────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Database connection ──────────────────────────────────────────────────────
DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    print("ERROR: DATABASE_URL environment variable is not set.")
    print("Set it or create a .env file with DATABASE_URL=postgresql://...")
    sys.exit(1)


def load_data():
    """Load all job postings from Supabase into a pandas DataFrame."""
    import psycopg2
    conn = psycopg2.connect(DB_URL)
    query = """
        SELECT title, company, location, experience, job_type,
               salary, career_level, date_posted, keywords,
               description, url, skills,
               date_first_seen, date_last_seen, status
        FROM job_postings
        WHERE status = 'active'
        ORDER BY date_first_seen DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def save_local_csv(df):
    """Save a timestamped CSV snapshot locally for monitoring."""
    os.makedirs("output", exist_ok=True)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    filepath = f"output/job_market_snapshot_{today}.csv"
    df.to_csv(filepath, index=False, encoding="utf-8")
    print(f"✅ Local CSV saved: {filepath} ({len(df)} rows)")
    return filepath


def basic_stats(df):
    """Print basic dataset statistics."""
    print("\n" + "=" * 60)
    print("📊 EGYPT JOB MARKET — DATASET OVERVIEW")
    print("=" * 60)
    print(f"  Total active job postings : {len(df):,}")
    print(f"  Unique companies         : {df['company'].nunique():,}")
    print(f"  Unique locations         : {df['location'].nunique():,}")
    print(f"  Date range               : {df['date_first_seen'].min()} → {df['date_first_seen'].max()}")
    print(f"  Columns                  : {list(df.columns)}")
    print()

    # Missing data summary
    print("📋 Missing Data Summary:")
    for col in ['experience', 'job_type', 'salary', 'career_level', 'description', 'skills']:
        if col in df.columns:
            if col == 'skills':
                missing = df[col].apply(lambda x: x is None or (isinstance(x, list) and len(x) == 0)).sum()
            elif col == 'salary':
                missing = df[col].isin(['', 'Not Specified', None]).sum()
            else:
                missing = df[col].isin(['', None]).sum()
            pct = (missing / len(df)) * 100 if len(df) > 0 else 0
            print(f"  {col:20s} : {missing:5d} missing ({pct:.1f}%)")
    print()


def top_companies(df, top_n=20):
    """Analyze and plot top hiring companies."""
    print(f"\n🏢 TOP {top_n} HIRING COMPANIES:")
    print("-" * 40)
    company_counts = df['company'].value_counts().head(top_n)
    for i, (company, count) in enumerate(company_counts.items(), 1):
        print(f"  {i:2d}. {company:35s} — {count:3d} openings")

    fig, ax = plt.subplots(figsize=(12, 8))
    company_counts.plot(kind='barh', ax=ax, color=sns.color_palette("viridis", top_n))
    ax.set_xlabel("Number of Open Positions")
    ax.set_title(f"Top {top_n} Hiring Companies in Egypt")
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig("output/top_companies.png", dpi=150)
    plt.close()
    print("  📈 Chart saved: output/top_companies.png")


def skill_analysis(df, top_n=25):
    """Explode the skills array and analyze frequency."""
    print(f"\n🔧 TOP {top_n} IN-DEMAND SKILLS:")
    print("-" * 40)

    # Explode skills array
    skills_series = df['skills'].dropna().explode()
    skills_series = skills_series[skills_series != '']
    skill_counts = skills_series.value_counts().head(top_n)

    for i, (skill, count) in enumerate(skill_counts.items(), 1):
        pct = (count / len(df)) * 100
        print(f"  {i:2d}. {skill:20s} — {count:4d} mentions ({pct:.1f}% of jobs)")

    fig, ax = plt.subplots(figsize=(14, 8))
    colors = sns.color_palette("magma", top_n)
    skill_counts.plot(kind='barh', ax=ax, color=colors)
    ax.set_xlabel("Frequency")
    ax.set_title(f"Top {top_n} Most Demanded Skills in Egypt's Job Market")
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig("output/top_skills.png", dpi=150)
    plt.close()
    print("  📈 Chart saved: output/top_skills.png")

    return skill_counts


def location_analysis(df, top_n=15):
    """Analyze job distribution by location."""
    print(f"\n📍 TOP {top_n} LOCATIONS:")
    print("-" * 40)
    loc_counts = df['location'].value_counts().head(top_n)
    for i, (loc, count) in enumerate(loc_counts.items(), 1):
        print(f"  {i:2d}. {loc:30s} — {count:3d} jobs")

    fig, ax = plt.subplots(figsize=(10, 6))
    loc_counts.plot(kind='bar', ax=ax, color=sns.color_palette("coolwarm", top_n))
    ax.set_ylabel("Number of Jobs")
    ax.set_title("Job Distribution by Location")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig("output/location_distribution.png", dpi=150)
    plt.close()
    print("  📈 Chart saved: output/location_distribution.png")


def career_level_analysis(df):
    """Analyze career level distribution."""
    print("\n🎯 CAREER LEVEL DISTRIBUTION:")
    print("-" * 40)
    career_df = df[df['career_level'].notna() & (df['career_level'] != '')]
    if career_df.empty:
        print("  No career level data available.")
        return

    level_counts = career_df['career_level'].value_counts()
    for level, count in level_counts.items():
        pct = (count / len(career_df)) * 100
        print(f"  {level:25s} — {count:3d} ({pct:.1f}%)")

    fig, ax = plt.subplots(figsize=(8, 8))
    level_counts.plot(kind='pie', ax=ax, autopct='%1.1f%%',
                      colors=sns.color_palette("Set2", len(level_counts)))
    ax.set_ylabel("")
    ax.set_title("Career Level Distribution")
    plt.tight_layout()
    plt.savefig("output/career_level_pie.png", dpi=150)
    plt.close()
    print("  📈 Chart saved: output/career_level_pie.png")


def job_type_analysis(df):
    """Analyze job type breakdown (Full Time, Part Time, etc.)."""
    print("\n💼 JOB TYPE BREAKDOWN:")
    print("-" * 40)
    type_df = df[df['job_type'].notna() & (df['job_type'] != '')]
    if type_df.empty:
        print("  No job type data available.")
        return

    type_counts = type_df['job_type'].value_counts().head(10)
    for jtype, count in type_counts.items():
        print(f"  {jtype:30s} — {count:3d}")

    fig, ax = plt.subplots(figsize=(10, 6))
    type_counts.plot(kind='bar', ax=ax, color=sns.color_palette("pastel", len(type_counts)))
    ax.set_ylabel("Count")
    ax.set_title("Job Type Breakdown")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig("output/job_type_breakdown.png", dpi=150)
    plt.close()
    print("  📈 Chart saved: output/job_type_breakdown.png")


def salary_analysis(df):
    """Analyze salary data where available."""
    print("\n💰 SALARY INSIGHTS:")
    print("-" * 40)
    salary_df = df[
        df['salary'].notna() &
        ~df['salary'].isin(['', 'Not Specified', 'Confidential'])
    ]
    print(f"  Jobs with salary info: {len(salary_df)} / {len(df)} ({(len(salary_df)/len(df)*100):.1f}%)")
    if not salary_df.empty:
        print(f"\n  Sample salary ranges:")
        for _, row in salary_df.head(10).iterrows():
            print(f"    {row['title'][:40]:40s} — {row['salary']}")


def hiring_trends(df):
    """Plot hiring trends over time."""
    print("\n📈 HIRING TRENDS:")
    print("-" * 40)
    df['date_first_seen'] = pd.to_datetime(df['date_first_seen'])
    daily = df.groupby(df['date_first_seen'].dt.date).size()
    cumulative = daily.cumsum()

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

    # Daily new jobs
    daily.plot(ax=ax1, marker='o', color='#e74c3c', linewidth=2)
    ax1.set_title("Daily New Job Postings")
    ax1.set_ylabel("New Jobs")
    ax1.grid(True, alpha=0.3)

    # Cumulative
    cumulative.plot(ax=ax2, color='#2ecc71', linewidth=2)
    ax2.fill_between(cumulative.index, cumulative.values, alpha=0.3, color='#2ecc71')
    ax2.set_title("Cumulative Job Postings Over Time")
    ax2.set_ylabel("Total Jobs")
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("output/hiring_trends.png", dpi=150)
    plt.close()
    print(f"  First scrape  : {daily.index[0]}")
    print(f"  Latest scrape : {daily.index[-1]}")
    print(f"  Total days    : {(daily.index[-1] - daily.index[0]).days}")
    print(f"  Avg jobs/day  : {daily.mean():.1f}")
    print("  📈 Chart saved: output/hiring_trends.png")


def skill_cooccurrence(df, top_n=15):
    """Analyze which skills appear together most often."""
    print(f"\n🔗 SKILL CO-OCCURRENCE (Top {top_n} skills):")
    print("-" * 40)

    skills_series = df['skills'].dropna()
    skills_series = skills_series[skills_series.apply(lambda x: isinstance(x, list) and len(x) >= 2)]

    if skills_series.empty:
        print("  Not enough multi-skill data for co-occurrence analysis.")
        return

    # Get top skills
    all_skills = skills_series.explode()
    top_skills = all_skills.value_counts().head(top_n).index.tolist()

    # Build co-occurrence matrix
    matrix = pd.DataFrame(0, index=top_skills, columns=top_skills)
    for skills_list in skills_series:
        filtered = [s for s in skills_list if s in top_skills]
        for i, s1 in enumerate(filtered):
            for s2 in filtered[i+1:]:
                matrix.loc[s1, s2] += 1
                matrix.loc[s2, s1] += 1

    fig, ax = plt.subplots(figsize=(14, 12))
    sns.heatmap(matrix, annot=True, fmt='d', cmap='YlOrRd', ax=ax,
                square=True, linewidths=0.5)
    ax.set_title("Skill Co-occurrence Heatmap")
    plt.tight_layout()
    plt.savefig("output/skill_cooccurrence.png", dpi=150)
    plt.close()
    print("  📈 Heatmap saved: output/skill_cooccurrence.png")


# ══════════════════════════════════════════════════════════════════════════════
#   MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("🚀 Loading data from Supabase PostgreSQL...")
    df = load_data()

    if df.empty:
        print("⚠️  No data found in the database. Run the scraper first!")
        print("    scrapy crawl wuzzuf_spider")
        sys.exit(0)

    # Save local CSV snapshot
    save_local_csv(df)

    # Run all analyses
    basic_stats(df)
    top_companies(df)
    skill_analysis(df)
    location_analysis(df)
    career_level_analysis(df)
    job_type_analysis(df)
    salary_analysis(df)
    hiring_trends(df)
    skill_cooccurrence(df)

    print("\n" + "=" * 60)
    print("✅ ANALYSIS COMPLETE")
    print(f"   All charts saved to output/")
    print(f"   Total jobs analyzed: {len(df):,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
