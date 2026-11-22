WITH base AS (
    SELECT * FROM {{ ref('jobs_clean') }}
)

SELECT
    year,
    month,
    source,

    COUNT(*) AS job_count,

    ROUND(AVG(salary_mid), 0) AS avg_salary,

    COUNT(salary_mid) AS jobs_with_salary,

    ROUND(
        100.0 * COUNT(salary_mid) / NULLIF(COUNT(*), 0),
        2
    ) AS salary_coverage_pct,

    COUNT(DISTINCT company) AS unique_companies

FROM base
GROUP BY year, month, source
ORDER BY year, month