
SELECT job_id, contract_type
FROM {{ ref('jobs_clean') }}
WHERE LOWER(contract_type) IN ('intérim', 'saisonnier', 'freelance', 'travail temporaire')
