WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_JOBS') }}
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

        -- Nettoyage salaire
        NULLIF(SALARY_MIN, 0)                       AS salary_min,
        NULLIF(SALARY_MAX, 0)                       AS salary_max,
        CASE
            WHEN SALARY_MIN > 0 AND SALARY_MAX > 0
            THEN (SALARY_MIN + SALARY_MAX) / 2
            ELSE NULL
        END                                         AS salary_mid,

        -- Dates déjà en TIMESTAMP_TZ, cast direct
        PUBLISHED_AT::TIMESTAMP_TZ                  AS published_at,
        COLLECTED_AT::TIMESTAMP_TZ                  AS collected_at,

        -- Dimensions temporelles
        YEAR(PUBLISHED_AT)                          AS year,
        MONTH(PUBLISHED_AT)                         AS month,
        WEEKOFYEAR(PUBLISHED_AT)                    AS week,

        SKILLS

    FROM source
    WHERE ID IS NOT NULL
      AND TITLE IS NOT NULL
)

SELECT * FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id, source ORDER BY collected_at DESC) = 1
