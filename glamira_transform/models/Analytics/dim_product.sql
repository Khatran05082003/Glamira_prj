WITH dim_product AS (
  SELECT 
      stg_raw.product_id,
      stg_product_name.name AS product_name
  FROM {{ ref('stg_raw') }} AS stg_raw
  LEFT JOIN {{ ref('stg_product_name') }} AS stg_product_name
    ON stg_product_name.id = stg_raw.product_id
)

SELECT DISTINCT
  product_id,
  product_name
FROM dim_product
WHERE product_id IS NOT NULL
  AND product_name IS NOT NULL
