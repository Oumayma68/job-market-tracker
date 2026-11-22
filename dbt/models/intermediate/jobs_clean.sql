WITH stg AS (
    SELECT * FROM {{ ref('stg_jobs') }}
)
SELECT
    j.job_id,
    j.job_title,
    j.title_norm,
    j.company,
    j.company_norm,
    j.location,
    j.location_norm,
    j.contract_type,
    j.source,
    j.url,
    j.salary_min,
    j.salary_max,
    j.salary_mid,
    j.published_at,
    j.collected_at,
    j.year,
    j.month,
    j.week,
    j.skills
FROM stg j
LEFT JOIN {{ ref('platforms') }} p
    ON LOWER(TRIM(j.company_norm)) = LOWER(TRIM(p.platform_name))
WHERE p.platform_name IS NULL
  AND j.company != ''