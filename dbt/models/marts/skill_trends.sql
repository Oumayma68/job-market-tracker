WITH jobs AS (
    SELECT year, month, skills
    FROM {{ ref('stg_jobs') }}
    WHERE skills IS NOT NULL
),

-- Snowflake : PARSE_JSON + FLATTEN pour exploser le tableau de skills
exploded AS (
    SELECT
        j.year,
        j.month,
        LOWER(TRIM(s.value::STRING)) AS skill
    FROM jobs j,
    LATERAL FLATTEN(input => PARSE_JSON(j.skills)) s
),

aggregated AS (
    SELECT
        skill,
        year,
        month,
        COUNT(*) AS mention_count
    FROM exploded
    WHERE skill != ''
    GROUP BY skill, year, month
),

-- Rank des skills par mois (utile pour streamlit Top N)
with_rank AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY year, month ORDER BY mention_count DESC) AS monthly_rank
    FROM aggregated
)

SELECT * FROM with_rank
ORDER BY year, month, monthly_rank
