WITH monthly AS (
    SELECT
        year,
        month,
        SUM(job_count) AS job_count
    FROM {{ ref('monthly_stats') }}
    GROUP BY year, month
),

with_trend AS (
    SELECT
        year,
        month,
        DATE_FROM_PARTS(year, month, 1) AS date,
        job_count,
        -- Moyenne mobile sur 3 mois
        ROUND(
            AVG(job_count) OVER (
                ORDER BY year, month
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ), 1
        ) AS rolling_avg_3m,
        -- % évolution mois sur mois
        ROUND(
            (job_count - LAG(job_count) OVER (ORDER BY year, month))
            / NULLIF(LAG(job_count) OVER (ORDER BY year, month), 0) * 100
        , 1) AS mom_growth_pct
    FROM monthly
)

SELECT * FROM with_trend
ORDER BY year, month
