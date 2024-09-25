WITH stg_raw AS (
  SELECT
    ip
  FROM {{ ref('stg_raw') }}
),

stg_location AS (
  SELECT
    zipcode
    ,timezone
    ,latitude
    ,region
    ,country_short
    ,longitude
    ,city
    ,country_long
    ,ip
  FROM {{ ref('stg_location') }}
),

dim_location AS (
  SELECT
    COALESCE(stg_location.zipcode, 'Unknown') AS zipcode
    ,COALESCE(stg_location.timezone, 'Unknown') AS timezone
    ,COALESCE(stg_location.latitude, -1.0) AS latitude
    ,COALESCE(stg_location.region, 'Unknown') AS region
    ,COALESCE(stg_location.country_short, 'Unknown') AS country_short
    ,COALESCE(stg_location.longitude, -1.0) AS longitude
    ,COALESCE(stg_location.city, 'Unknown') AS city
    ,COALESCE(stg_location.country_long, 'Unknown') AS country_long
    ,COALESCE(stg_location.ip, 'Unknown') AS ip
  FROM {{ ref('stg_location') }} AS stg_location
  RIGHT JOIN {{ ref('stg_raw') }} AS stg_raw
  ON stg_location.ip = stg_raw.ip
)

SELECT DISTINCT
  zipcode
  ,timezone
  ,latitude
  ,region
  ,country_short
  ,longitude
  ,city
  ,country_long
  ,ip
FROM dim_location
