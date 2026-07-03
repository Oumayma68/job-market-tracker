WITH source AS (
    SELECT *
    FROM {{ source('raw', 'RAW_JOBS') }}
),
cleaned AS (
    SELECT
        ID                                          AS job_id,
        TITLE                                       AS job_title,
        COMPANY                                     AS company,
        LOCATION                                    AS location,
        CONTRACT_TYPE                               AS contract_type,
        SOURCE                                      AS source,
        URL                                         AS url,
        NULLIF(SALARY_MIN, 0)                       AS salary_min,
        NULLIF(SALARY_MAX, 0)                       AS salary_max,
        CASE
            WHEN SALARY_MIN > 0 AND SALARY_MAX > 0 THEN (SALARY_MIN + SALARY_MAX) / 2
            WHEN SALARY_MIN > 0 THEN SALARY_MIN
            WHEN SALARY_MAX > 0 THEN SALARY_MAX
            ELSE NULL
        END                                         AS salary_mid,
        PUBLISHED_AT::TIMESTAMP_TZ                  AS published_at,
        COLLECTED_AT::TIMESTAMP_TZ                  AS collected_at,
        YEAR(PUBLISHED_AT::TIMESTAMP_TZ)            AS year,
        MONTH(PUBLISHED_AT::TIMESTAMP_TZ)           AS month,
        WEEKOFYEAR(PUBLISHED_AT::TIMESTAMP_TZ)      AS week,
        SKILLS
    FROM source
    WHERE ID IS NOT NULL
      AND TITLE IS NOT NULL
      AND COMPANY IS NOT NULL
      AND PUBLISHED_AT::TIMESTAMP_TZ <= CURRENT_TIMESTAMP()
      AND NOT (
          SALARY_MIN > 0
          AND SALARY_MAX > 0
          AND SALARY_MIN > SALARY_MAX
      )
),
normalized AS (
    SELECT
        *,
        REGEXP_REPLACE(
            LOWER(TRIM(job_title)),
            '( h/f| f/h| hf|\(h/f\)|\[h/f\]|/h/f| h\.f\.)',
            ''
        ) AS title_norm,
        REGEXP_REPLACE(LOWER(TRIM(company)), '\s+', ' ')  AS company_norm,
        REGEXP_REPLACE(LOWER(TRIM(location)), '\s+', ' ') AS location_norm
    FROM cleaned
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY collected_at DESC) AS rn
    FROM normalized
)
SELECT
    job_id,
    job_title,
    title_norm,
    company,
    company_norm,
    location,
    location_norm,
    contract_type,
    source,
    url,
    salary_min,
    salary_max,
    salary_mid,
    published_at,
    collected_at,
    year,
    month,
    week,
    skills
FROM deduped
WHERE rn = 1