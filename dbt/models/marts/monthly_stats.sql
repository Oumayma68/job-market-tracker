WITH base AS (
    SELECT * FROM {{ ref('jobs_clean') }}
)
SELECT
    year,
    month,
    source,
    COUNT(*)                    AS job_count,
    ROUND(AVG(salary_mid), 0)   AS avg_salary,
    COUNT(DISTINCT company)     AS unique_companies
FROM base
GROUP BY year, month, source
ORDER BY year, month