SELECT *
FROM {{ source('raw', 'RAW_JOBS') }}
WHERE published_at > CURRENT_TIMESTAMP()
 
