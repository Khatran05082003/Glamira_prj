WITH stg_table AS (
  SELECT 
    *
  FROM `glamira-prj.glamira_dataset.json_product_name_ndjson` 
),
stg_table__rename__cast_type AS (
  SELECT
    CASE
      WHEN CAST(name AS STRING) IS NULL OR CAST(name AS STRING) = '-' THEN 'Unknown'
      ELSE CAST(name AS STRING)
    END AS name
    ,CASE
      WHEN CAST(id AS STRING) IS NULL OR CAST(id AS STRING) = '-' THEN -1
      ELSE CAST(id AS INTEGER)
    END AS id
  FROM
    stg_table
)

,stg_table__add_undefined_record AS (
  SELECT 
    *
  FROM stg_table__rename__cast_type

  UNION ALL

  SELECT
    'Unknown' AS name
    ,-1 AS id
)


SELECT
    *
FROM stg_table__add_undefined_record
