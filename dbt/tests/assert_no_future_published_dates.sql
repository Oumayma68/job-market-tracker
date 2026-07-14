SELECT *
FROM {{ ref('stg_jobs') }}
WHERE published_at > CURRENT_TIMESTAMP()
 
