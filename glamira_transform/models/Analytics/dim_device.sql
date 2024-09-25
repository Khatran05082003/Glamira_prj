WITH dim_device AS (
  SELECT DISTINCT
    stg_raw.device_id AS device_id
    ,stg_raw.resolution AS resolution
    ,stg_raw.user_agent AS user_agent
    ,stg_raw.api_version AS api_version
  FROM {{ ref('stg_raw') }} AS stg_raw
)

SELECT
   *
FROM dim_device
