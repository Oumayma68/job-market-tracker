WITH jobs AS (
    SELECT salary_mid, skills
    FROM {{ ref('jobs_clean') }}
    WHERE skills IS NOT NULL
      AND salary_mid IS NOT NULL
),
exploded AS (
    SELECT
        j.salary_mid,
        LOWER(TRIM(s.value::STRING)) AS skill
    FROM jobs j,
    LATERAL FLATTEN(input => PARSE_JSON(j.skills)) s
),
aggregated AS (
    SELECT
        skill,
        COUNT(*)                    AS mention_count,
        ROUND(AVG(salary_mid), 0)   AS avg_salary,
        ROUND(MIN(salary_mid), 0)   AS min_salary,
        ROUND(MAX(salary_mid), 0)   AS max_salary
    FROM exploded
    WHERE skill != ''
    GROUP BY skill
    HAVING COUNT(*) >= 5
)
SELECT * FROM aggregated
ORDER BY avg_salary DESC