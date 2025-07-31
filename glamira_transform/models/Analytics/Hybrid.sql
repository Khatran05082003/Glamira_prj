WITH collections_with_valid_email AS (
  SELECT 
    collection,
    MAX(CASE WHEN email_address IS NOT NULL AND email_address != '' THEN 1 ELSE 0 END) AS has_valid_email
  FROM `project2-423018.glamira_dataset.summary`
  GROUP BY collection
  HAVING MAX(CASE WHEN email_address IS NOT NULL AND email_address != '' THEN 1 ELSE 0 END) = 1
),

summary_with_flags AS (
  SELECT
    s.collection,
    COUNTIF(s.product_id IS NULL) AS null_product_id_count,
    MAX(CASE 
        WHEN ARRAY_LENGTH(
          ARRAY(
            SELECT 1
            FROM UNNEST(s.cart_products) AS cp
            WHERE cp.product_id IS NOT NULL
          )
        ) > 0 THEN 1 ELSE 0 END) AS has_valid_cart_product
  FROM `project2-423018.glamira_dataset.summary` s
  WHERE s.collection IN (SELECT collection FROM collections_with_valid_email)
  GROUP BY s.collection
),


valid_collections AS (
  SELECT collection
  FROM summary_with_flags
  WHERE null_product_id_count = 0 OR has_valid_cart_product = 1
),


base_data AS (
  SELECT 
    s.email_address,
    s.currency,
    COALESCE(s.product_id, cp.product_id) AS final_product_id,
    s.ip,
    CAST(TIMESTAMP_SECONDS(CAST (s.time_stamp AS INT64)) AS DATE) AS full_date,
  COALESCE(
    CASE
      WHEN REGEXP_CONTAINS(TRIM(s.price), r'^[0-9]{1,3}(\.[0-9]{3})*,[0-9]{2}$') THEN 
          CAST(REPLACE(REPLACE(TRIM(s.price), '.', ''), ',', '.') AS FLOAT64)

      WHEN REGEXP_CONTAINS(TRIM(s.price), r'^[0-9]{1,3}(,[0-9]{3})*\.[0-9]{2}$') THEN 
          CAST(REPLACE(TRIM(s.price), ',', '') AS FLOAT64)

      WHEN REGEXP_CONTAINS(TRIM(s.price), r"^\d+'?\d*\.\d{2}$") THEN 
          CAST(REPLACE(TRIM(s.price), "'", "") AS FLOAT64)

      WHEN REGEXP_CONTAINS(TRIM(s.price), r'^\d+(\.\d+)?$') THEN 
          CAST(TRIM(s.price) AS FLOAT64)

      WHEN REGEXP_CONTAINS(TRIM(s.price), r'^\d{1,3}(,\d{3})*$') THEN 
          CAST(REPLACE(TRIM(s.price), ',', '') AS FLOAT64)

      WHEN REGEXP_CONTAINS(TRIM(s.price), r'^\d+[\.٫]\d{2}$') THEN 
          CAST(REGEXP_REPLACE(TRIM(s.price), r'[٫.]', '') AS FLOAT64)

      ELSE 0
    END,

    
    CASE
      WHEN REGEXP_CONTAINS(TRIM(cp.price), r'^[0-9]{1,3}(\.[0-9]{3})*,[0-9]{2}$') THEN 
          CAST(REPLACE(REPLACE(TRIM(cp.price), '.', ''), ',', '.') AS FLOAT64)

      WHEN REGEXP_CONTAINS(TRIM(cp.price), r'^[0-9]{1,3}(,[0-9]{3})*\.[0-9]{2}$') THEN 
          CAST(REPLACE(TRIM(cp.price), ',', '') AS FLOAT64)

      WHEN REGEXP_CONTAINS(TRIM(cp.price), r"^\d+'?\d*\.\d{2}$") THEN 
          CAST(REPLACE(TRIM(cp.price), "'", "") AS FLOAT64)

      WHEN REGEXP_CONTAINS(TRIM(cp.price), r'^\d+(\.\d+)?$') THEN 
          CAST(TRIM(cp.price) AS FLOAT64)

      WHEN REGEXP_CONTAINS(TRIM(cp.price), r'^\d{1,3}(,\d{3})*$') THEN 
          CAST(REPLACE(TRIM(cp.price), ',', '') AS FLOAT64)

      WHEN REGEXP_CONTAINS(TRIM(cp.price), r'^\d+[\.٫]\d{2}$') THEN 
          CAST(REGEXP_REPLACE(TRIM(cp.price), r'[٫.]', '') AS FLOAT64)

      ELSE 0
    END
  ) AS price,

    
    ARRAY(
      SELECT AS STRUCT
        CASE 
          WHEN o.option_label IS NOT NULL THEN o.option_label
          ELSE JSON_EXTRACT_SCALAR(cp.option, '$.option_label')
        END AS option_label,
        CASE 
          WHEN o.option_id IS NOT NULL THEN o.option_id
          ELSE JSON_EXTRACT_SCALAR(cp.option, '$.option_id')
        END AS option_id,
        CASE 
          WHEN o.value_id IS NOT NULL THEN o.value_id
          ELSE JSON_EXTRACT_SCALAR(cp.option, '$.value_id')
        END AS value_id
      FROM UNNEST(COALESCE(s.option, [])) AS o
      LIMIT 1  
    ) AS option,
    s.collection
  FROM `project2-423018.glamira_dataset.summary` s
  LEFT JOIN UNNEST(s.cart_products) AS cp
  WHERE s.collection IN (
    SELECT collection FROM valid_collections
  )
  AND (s.product_id IS NOT NULL OR cp.product_id IS NOT NULL)
  AND (s.email_address IS NOT NULL AND s.email_address != '')
)


SELECT 
  b.* EXCEPT(price, currency),
  b.price * IFNULL(c.usd_conversion_rate, 1) AS final_price,
  p.product_name
FROM base_data b
LEFT JOIN {{ ref('dim_location') }} l
  ON b.ip = l.ip
LEFT JOIN {{ ref('dim_currency') }} c
  ON c.currency = b.currency
LEFT JOIN {{ ref('dim_product') }} p
  ON p.product_id = b.final_product_id
WHERE final_product_id IN (
  SELECT product_id FROM {{ ref('dim_product') }}
)