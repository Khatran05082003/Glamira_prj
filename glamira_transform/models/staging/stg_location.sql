WITH stg_location AS (
  SELECT 
    *
  FROM `glamira-prj.glamira_dataset.location`
),
stg_location__rename__cast_type AS (
  SELECT
    CASE
      WHEN CAST(zipcode AS STRING) IS NULL OR CAST(zipcode AS STRING) = '-' THEN 'Unknown'
      ELSE CAST(zipcode AS STRING)
    END AS zipcode
    ,CASE
      WHEN CAST(timezone AS STRING) IS NULL OR CAST(timezone AS STRING) = '-' THEN 'Unknown'
      ELSE CAST(timezone AS STRING)
    END AS timezone
    ,CASE
      WHEN CAST(latitude AS STRING) IS NULL OR CAST(latitude AS STRING) = '-' THEN -1.0
      ELSE CAST(latitude AS FLOAT64)
    END AS latitude
    ,CASE
      WHEN CAST(region AS STRING) IS NULL OR CAST(region AS STRING) = '-' THEN 'Unknown'
      ELSE CAST(region AS STRING)
    END AS region
    ,CASE
      WHEN CAST(country_short AS STRING) IS NULL OR CAST(country_short AS STRING) = '-' THEN 'Unknown'
      ELSE CAST(country_short AS STRING)
    END AS country_short
    ,CASE
      WHEN CAST(longitude AS STRING) IS NULL OR CAST(longitude AS STRING) = '-' THEN -1.0
      ELSE CAST(longitude AS FLOAT64)
    END AS longitude
    ,CASE
      WHEN CAST(city AS STRING) IS NULL OR CAST(city AS STRING) = '-' THEN 'Unknown'
      ELSE CAST(city AS STRING)
    END AS city
    ,CASE
      WHEN CAST(country_long AS STRING) IS NULL OR CAST(country_long AS STRING) = '-' THEN 'Unknown'
      ELSE CAST(country_long AS STRING)
    END AS country_long
    ,CASE
      WHEN CAST(ip AS STRING) IS NULL OR CAST(ip AS STRING) = '-' THEN 'Unknown'
      ELSE CAST(ip AS STRING)
    END AS ip
  FROM
    stg_location
)

,stg_location__add_undefined_record AS (
  SELECT 
    *
  FROM stg_location__rename__cast_type

  UNION ALL

  SELECT
    'Unknown' AS zipcode
    ,'Unknown' AS timezone
    ,0.0 AS latitude
    ,'Unknown' AS region
    ,'Unknown' AS country_short
    ,0.0 AS longitude
    ,'Unknown' AS city
    ,'Unknown' AS country_long
    ,'Unknown' AS ip
)


SELECT
    *
FROM stg_location__add_undefined_record
