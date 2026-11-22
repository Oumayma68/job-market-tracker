SELECT *
FROM {{ source('raw', 'RAW_JOBS') }}
WHERE salary_min IS NOT NULL
  AND salary_max IS NOT NULL
  AND salary_min > salary_max
 
