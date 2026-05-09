ww-- ============================================================
-- Silver Layer: job_postings_clean
-- Run this once in your Supabase SQL Editor
-- ============================================================

CREATE TABLE IF NOT EXISTS public.job_postings_clean (
    -- Primary key
    clean_hash          TEXT PRIMARY KEY,

    -- FK to Bronze
    source_hash         TEXT NOT NULL REFERENCES public.job_postings(job_hash) ON DELETE CASCADE,

    -- Original fields (cleaned)
    title               TEXT,
    company             TEXT,
    location            TEXT,
    experience          TEXT,
    job_type            TEXT,
    salary              TEXT,
    career_level        TEXT,
    date_posted         TEXT,
    url                 TEXT,
    description         TEXT,
    date_first_seen     DATE,
    date_last_seen      DATE,
    historical_date     DATE,
    status              TEXT DEFAULT 'active',

    -- Normalized fields
    title_clean         TEXT,
    job_family          TEXT,
    seniority_level     TEXT,   -- Intern / Entry / Mid / Senior / Executive
    city                TEXT,
    governorate         TEXT,

    -- Extracted from experience field + description NLP
    years_exp_min       INT,
    years_exp_max       INT,

    -- Extracted from description NLP
    education_level     TEXT,   -- Bachelor's / Master's / Diploma / PhD / Not Specified
    salary_min          INT,    -- EGP, null if not found
    salary_max          INT,    -- EGP, null if not found

    -- Skills (original + description-inferred, merged)
    skills              TEXT[],
    inferred_skills     TEXT[],

    -- Timestamps
    normalized_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Index for common query patterns
CREATE INDEX IF NOT EXISTS idx_clean_job_family     ON public.job_postings_clean (job_family);
CREATE INDEX IF NOT EXISTS idx_clean_seniority      ON public.job_postings_clean (seniority_level);
CREATE INDEX IF NOT EXISTS idx_clean_governorate    ON public.job_postings_clean (governorate);
CREATE INDEX IF NOT EXISTS idx_clean_historical     ON public.job_postings_clean (historical_date);
CREATE INDEX IF NOT EXISTS idx_clean_status         ON public.job_postings_clean (status);
CREATE INDEX IF NOT EXISTS idx_clean_source_hash    ON public.job_postings_clean (source_hash);

-- ============================================================
-- Silver Views (Power BI connects to these)
-- ============================================================

-- Unnested skills (original + inferred, merged and deduplicated)
CREATE OR REPLACE VIEW public.v_clean_skills AS
SELECT
    c.clean_hash,
    c.job_family,
    c.seniority_level,
    c.historical_date,
    skill
FROM public.job_postings_clean c,
     UNNEST(
         ARRAY(
             SELECT DISTINCT s
             FROM UNNEST(
                 COALESCE(c.skills, '{}') || COALESCE(c.inferred_skills, '{}')
             ) AS s
         )
     ) AS skill
WHERE c.status = 'active';

-- Unnested job types
CREATE OR REPLACE VIEW public.v_clean_job_types AS
SELECT
    c.clean_hash,
    c.job_family,
    c.seniority_level,
    TRIM(job_type_individual) AS job_type_individual
FROM public.job_postings_clean c,
     UNNEST(STRING_TO_ARRAY(c.job_type, ',')) AS job_type_individual
WHERE c.status = 'active';
