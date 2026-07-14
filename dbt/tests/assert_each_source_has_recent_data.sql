SELECT
    source,
    MAX(collected_at) AS last_collected_at,
    DATEDIFF('day', MAX(collected_at), CURRENT_TIMESTAMP()) AS days_since_last_collect
FROM {{ ref('stg_jobs') }}
GROUP BY source
HAVING DATEDIFF('day', MAX(collected_at), CURRENT_TIMESTAMP()) > 2
