WITH fact_order AS (
  SELECT 
      GENERATE_UUID() AS order_line_key, 
      stg_raw.product_id AS product_key,
      stg_raw.user_id_db AS user_key,
      stg_raw.ip AS location_key,
      stg_raw.device_id AS device_key,
      stg_raw.full_date AS order_date,
      stg_raw.viewing_product_id,
      stg_raw.collect_id,
      stg_raw.collection,
      stg_raw.utm_source,
      stg_raw.recommendation,
      stg_raw.utm_medium,
      stg_raw.show_recommendation,
      stg_raw.is_paypal,
      stg_raw.key_search,
      stg_raw.price,
      stg_raw.currency,
      stg_raw.amount
  FROM {{ ref('stg_raw') }} AS stg_raw
  LEFT JOIN {{ ref('dim_product') }} AS dim_product
    ON stg_raw.product_id = dim_product.product_id
  LEFT JOIN {{ ref('dim_location') }} AS dim_location
    ON stg_raw.ip = dim_location.ip
  LEFT JOIN {{ ref('dim_device') }} AS dim_device
    ON stg_raw.device_id = dim_device.device_id
  LEFT JOIN {{ ref('dim_date') }} AS dim_date
    ON stg_raw.full_date = dim_date.date
  LEFT JOIN {{ ref('dim_user') }} AS dim_user
    ON stg_raw.user_id_db = dim_user.user_id 
  LEFT JOIN {{ ref('dim_currency') }} AS dim_currency
    ON stg_raw.currency = dim_currency.currency
),

option_data AS (
  SELECT 
      stg_raw.product_id AS product_key,
      option.alloy,
      option.diamond,
      option.option_label,
      option.option_id AS option_id,
      option.value_label,
      option.value_id,
      option.quality,
      option.quality_label
  FROM {{ ref('stg_raw') }} AS stg_raw
  LEFT JOIN UNNEST(stg_raw.option) AS option
  LEFT JOIN {{ ref('dim_alloy') }} AS dim_alloy
    ON option.value_label = dim_alloy.alloy_value
),

fact_order_table AS (
    SELECT
        f.*,
        o.alloy,
        o.diamond,
        o.option_label,
        o.option_id,
        o.value_label,
        o.value_id,
        o.quality,
        o.quality_label
    FROM fact_order AS f
    LEFT JOIN option_data AS o
        ON f.product_key = o.product_key
)


SELECT DISTINCT *
FROM fact_order_table
