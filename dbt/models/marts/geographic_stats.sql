
WITH geographic_jobs AS (
    SELECT
        job_id,
        company,
        location_norm,
        salary_mid,
        published_at,

        CASE
            WHEN REGEXP_LIKE(
                TRIM(location_norm),
                '^[0-9]{2,3} - .+'
            )
            THEN TRIM(SUBSTR(
                location_norm,
                POSITION('-' IN location_norm) + 1
            ))
            ELSE TRIM(location_norm)
        END AS city

    FROM {{ ref('jobs_clean') }}
    WHERE location_norm IS NOT NULL
      AND TRIM(location_norm) != ''
),

aggregated AS (
    SELECT
        city,
        COUNT(DISTINCT job_id) AS job_count,
        COUNT(DISTINCT company) AS company_count,
        ROUND(AVG(salary_mid), 0) AS avg_salary,
        ROUND(MEDIAN(salary_mid), 0) AS median_salary,
        MIN(published_at) AS first_published_at,
        MAX(published_at) AS last_published_at
    FROM geographic_jobs
    GROUP BY city
)

SELECT *
FROM aggregated
ORDER BY job_count DESC
