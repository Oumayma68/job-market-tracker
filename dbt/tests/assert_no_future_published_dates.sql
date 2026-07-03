-- Test : pas d'offres avec une date de publication dans le futur
SELECT *
FROM {{ ref('stg_jobs') }}
WHERE published_at > CURRENT_TIMESTAMP()
 
