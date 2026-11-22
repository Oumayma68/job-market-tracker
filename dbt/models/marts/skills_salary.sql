
WITH jobs AS (
    SELECT
        job_id,
        salary_mid,
        skills
    FROM {{ ref('jobs_clean') }}
    WHERE skills IS NOT NULL
      AND salary_mid IS NOT NULL
),

exploded AS (
    SELECT
        j.job_id,
        j.salary_mid,
        LOWER(TRIM(s.value::STRING)) AS skill
    FROM jobs j,
    LATERAL FLATTEN(input => j.skills) s
),

deduplicated AS (
    SELECT DISTINCT
        job_id,
        salary_mid,
        skill
    FROM exploded
    WHERE skill IS NOT NULL
      AND skill != ''
),

aggregated AS (
    SELECT
        skill,
        COUNT(DISTINCT job_id) AS mention_count,
        ROUND(AVG(salary_mid), 0) AS avg_salary,
        ROUND(MEDIAN(salary_mid), 0) AS median_salary,
        ROUND(MIN(salary_mid), 0) AS min_salary,
        ROUND(MAX(salary_mid), 0) AS max_salary
    FROM deduplicated
    GROUP BY skill
    HAVING COUNT(DISTINCT job_id) >= 5
)

SELECT *
FROM aggregated
ORDER BY avg_salary DESC
