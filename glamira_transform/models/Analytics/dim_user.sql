WITH dim_user AS (
  SELECT 
      stg_raw.user_id_db AS user_id,
      stg_raw.email_address AS email
  FROM {{ ref('stg_raw') }} AS stg_raw
)

SELECT DISTINCT
   user_id,
   email
FROM dim_user
WHERE user_id IS NOT NULL
  AND email IS NOT NULL
