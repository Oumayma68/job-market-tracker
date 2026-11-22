
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

        -- Salaires : neutralisation des valeurs invalides
        CASE
            WHEN SALARY_MIN > 0
             AND SALARY_MAX > 0
             AND SALARY_MIN > SALARY_MAX
                THEN NULL
            WHEN SALARY_MIN > 0
                THEN SALARY_MIN
            ELSE NULL
        END                                         AS salary_min,

        CASE
            WHEN SALARY_MIN > 0
             AND SALARY_MAX > 0
             AND SALARY_MIN > SALARY_MAX
                THEN NULL
            WHEN SALARY_MAX > 0
                THEN SALARY_MAX
            ELSE NULL
        END                                         AS salary_max,

        PUBLISHED_AT::TIMESTAMP_TZ                  AS published_at,
        COLLECTED_AT::TIMESTAMP_TZ                  AS collected_at,
        YEAR(PUBLISHED_AT::TIMESTAMP_TZ)            AS year,
        MONTH(PUBLISHED_AT::TIMESTAMP_TZ)           AS month,
        WEEKOFYEAR(PUBLISHED_AT::TIMESTAMP_TZ)      AS week,
        SKILLS                                      AS skills

    FROM source

    WHERE ID IS NOT NULL
      AND TITLE IS NOT NULL
      AND COMPANY IS NOT NULL
      AND PUBLISHED_AT::TIMESTAMP_TZ <= CURRENT_TIMESTAMP()
),

with_mid AS (
    SELECT
        *,
        CASE
            WHEN salary_min IS NOT NULL
             AND salary_max IS NOT NULL
                THEN (salary_min + salary_max) / 2
            ELSE COALESCE(salary_min, salary_max)
        END AS salary_mid
    FROM cleaned
),

normalized AS (
    SELECT
        *,
        REGEXP_REPLACE(
            LOWER(TRIM(job_title)),
            '( h/f| f/h| hf|\(h/f\)|\[h/f\]|/h/f| h\.f\.)',
            ''
        ) AS title_norm,

        REGEXP_REPLACE(
            LOWER(TRIM(company)),
            '\\s+',
            ' '
        ) AS company_norm,

        REGEXP_REPLACE(
            LOWER(TRIM(location)),
            '\\s+',
            ' '
        ) AS location_norm

    FROM with_mid
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

FROM normalized
