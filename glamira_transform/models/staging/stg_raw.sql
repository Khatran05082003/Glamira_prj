WITH RawData AS (
  SELECT
    IFNULL(viewing_product_id, 'Unknown') AS viewing_product_id,
    IFNULL(user_agent, 'Unknown') AS user_agent,
    IFNULL(collect_id, 'Unknown') AS collect_id,
    IFNULL(product_id, -1) AS product_id,  
    IFNULL(collection, 'Unknown') AS collection,
    IFNULL(utm_source, 'Unknown') AS utm_source,
    IFNULL(recommendation, 'Unknown') AS recommendation,
    IFNULL(utm_medium, 'Unknown') AS utm_medium,
    IFNULL(show_recommendation, 'Unknown') AS show_recommendation,
    IFNULL(user_id_db, 'Unknown') AS user_id_db,
    SAFE_CAST(IFNULL(local_time, '1970-01-01 00:00:00') AS TIMESTAMP) AS local_time,
    IFNULL(email_address, 'Unknown') AS email_address,
    IFNULL(referrer_url, 'Unknown') AS referrer_url,
    IFNULL(cat_id, 'Unknown') AS cat_id,
    IFNULL(is_paypal, FALSE) AS is_paypal,
    IFNULL(device_id, 'Unknown') AS device_id,
    IFNULL(key_search, 'Unknown') AS key_search,
    IFNULL(current_url, 'Unknown') AS current_url,
    IFNULL(api_version, 'Unknown') AS api_version,
    IFNULL(resolution, 'Unknown') AS resolution,
    IFNULL(ip, 'Unknown') AS ip,
    IFNULL(store_id, 'Unknown') AS store_id,
    CAST(REGEXP_EXTRACT(TO_JSON_STRING(time_stamp), r'"\$numberInt":"(\d+)"') AS STRING) AS time_stamp,
    CAST(TIMESTAMP_SECONDS(CAST(REGEXP_EXTRACT(TO_JSON_STRING(time_stamp), r'"\$numberInt":"(\d+)"') AS INT64)) AS DATE) AS full_date,
    
    -- Tách dữ liệu 'option' thành các trường con
    ARRAY(
      SELECT AS STRUCT
        NULLIF(option.alloy, "Unknown") AS alloy,
        NULLIF(option.diamond, "Unknown") AS diamond,
        NULLIF(option.option_label, "Unknown") AS option_label,
        NULLIF(option.option_id, "Unknown") AS option_id,
        NULLIF(option.value_label, "Unknown") AS value_label,
        NULLIF(option.value_id, "Unknown") AS value_id,
        NULLIF(option.quality, "Unknown") AS quality,
        NULLIF(option.quality_label, "Unknown") AS quality_label
      FROM UNNEST(option) AS option
    ) AS option,
    
    -- Tách dữ liệu 'cart_products' thành các trường con
    ARRAY(
      SELECT AS STRUCT
        CASE
          -- Format with dot as thousand separator and comma as decimal separator (e.g., 10.497.373,00)
          WHEN REGEXP_CONTAINS(TRIM(cart_products.price), r'^[0-9]{1,3}(\.[0-9]{3})*,[0-9]{2}$') THEN 
              CAST(REPLACE(REPLACE(TRIM(cart_products.price), '.', ''), ',', '.') AS FLOAT64)

          -- Format with comma as thousand separator and dot as decimal separator (e.g., 1,234.56)
          WHEN REGEXP_CONTAINS(TRIM(cart_products.price), r'^[0-9]{1,3}(,[0-9]{3})*\.[0-9]{2}$') THEN 
              CAST(REPLACE(TRIM(cart_products.price), ',', '') AS FLOAT64)

          -- Format with apostrophe as thousand separator and dot as decimal separator (e.g., 3'583.00)
          WHEN REGEXP_CONTAINS(TRIM(cart_products.price), r"^\d+'?\d*\.\d{2}$") THEN 
              CAST(REPLACE(TRIM(cart_products.price), "'", "") AS FLOAT64)

          -- Format with simple decimal (e.g., 1234.56 or 1234)
          WHEN REGEXP_CONTAINS(TRIM(cart_products.price), r'^\d+(\.\d+)?$') THEN 
              CAST(TRIM(cart_products.price) AS FLOAT64)

          -- Handle case like '20,933' converting to '20933'
          WHEN REGEXP_CONTAINS(TRIM(cart_products.price), r'^\d{1,3}(,\d{3})*$') THEN 
              CAST(REPLACE(TRIM(cart_products.price), ',', '') AS FLOAT64)

          -- Handle case like '61٫00' converting to '61'
          WHEN REGEXP_CONTAINS(TRIM(cart_products.price), r'^\d+[\.٫]\d{2}$') THEN 
              CAST(TRIM(REGEXP_REPLACE(cart_products.price, r'[٫.]', '')) AS FLOAT64)

          ELSE
              0  
        END AS price,
        COALESCE(NULLIF(TRIM(CAST(cart_products.currency AS STRING)), ""), "Unknown") AS currency,
        NULLIF(CAST(REGEXP_EXTRACT(TO_JSON_STRING(cart_products.amount), r'"\$numberInt":"(\d+)"') AS INT64), 0) AS amount,
        CAST(REGEXP_EXTRACT(TO_JSON_STRING(cart_products.product_id), r'"\$numberInt":"(\d+)"') AS INT64) AS product_id,
        ARRAY(
          SELECT AS STRUCT
            CAST(REGEXP_EXTRACT(TO_JSON_STRING(option), r'"option_id"\:\{"\$numberInt":"(\d+)"\}') AS STRING) AS option_id,
            CAST(REGEXP_EXTRACT(TO_JSON_STRING(option), r'"value_id"\:\{"\$numberInt":"(\d+)"\}') AS STRING) AS value_id,
            CASE
                WHEN JSON_EXTRACT_SCALAR(TO_JSON_STRING(option), '$.option_label') = 'diamond' THEN 
                  JSON_EXTRACT_SCALAR(TO_JSON_STRING(option), '$.value_label')
                ELSE 'Unknown'
              END AS value_label_diamond,
              CASE
                WHEN JSON_EXTRACT_SCALAR(TO_JSON_STRING(option), '$.option_label') = 'alloy' THEN 
                  JSON_EXTRACT_SCALAR(TO_JSON_STRING(option), '$.value_label')
                ELSE 'Unknown'
              END AS value_label_alloy
          FROM UNNEST(JSON_EXTRACT_ARRAY(option)) AS option
        ) AS option
        FROM UNNEST(cart_products) AS cart_products
    ) AS cart_products
  FROM
    main-cocoa-445214-r4.glamira_dataset.summary
),

stg_glamira_raw__add_undefined_record AS (
  SELECT 
    viewing_product_id,
    user_agent,
    collect_id,
    product_id,  
    collection,
    utm_source,
    recommendation,
    utm_medium,
    show_recommendation,
    user_id_db,
    local_time,
    email_address,
    referrer_url,
    cat_id,
    is_paypal,
    device_id,
    key_search,
    current_url,
    api_version,
    resolution,
    ip,
    store_id,
    time_stamp,
    full_date,
    option,
    cart_products
  FROM RawData

  UNION ALL

  SELECT
    'Unknown' AS viewing_product_id,
    'Unknown' AS user_agent,
    'Unknown' AS collect_id,
    -1 AS product_id, 
    'Unknown' AS collection,
    'Unknown' AS utm_source,
    'Unknown' AS recommendation,
    'Unknown' AS utm_medium,
    'Unknown' AS show_recommendation,
    'Unknown' AS user_id_db,
    TIMESTAMP('1970-01-01 00:00:00') AS local_time,  
    'Unknown' AS email_address,
    'Unknown' AS referrer_url,
    'Unknown' AS cat_id,
    FALSE AS is_paypal,
    'Unknown' AS device_id,
    'Unknown' AS key_search,
    'Unknown' AS current_url,
    'Unknown' AS api_version,
    'Unknown' AS resolution,
    'Unknown' AS ip,
    'Unknown' AS store_id,
    'Unknown' AS time_stamp,
    DATE('1970-01-01') AS full_date,
    
    -- Giá trị mặc định cho 'option'
    ARRAY(
      SELECT AS STRUCT
        'Unknown' AS alloy,
        'Unknown' AS diamond,
        'Unknown' AS option_label,
        'Unknown' AS option_id,
        'Unknown' AS value_label,
        'Unknown' AS value_id,
        'Unknown' AS quality,
        'Unknown' AS quality_label
    ) AS option,
    
    -- Giá trị mặc định cho 'cart_products'
    ARRAY(
      SELECT AS STRUCT
        CAST(0 AS FLOAT64) AS price, 
        'Unknown' AS currency,
        CAST(0 AS INT64) AS amount,
        CAST(-1 AS INT64) AS product_id,
        ARRAY(
          SELECT AS STRUCT
            'Unknown' AS option_id,
            'Unknown' AS value_id,
            'Unknown' AS value_label_diamond,
            'Unknown' AS value_label_alloy
        ) AS options
      ) AS cart_products

)

SELECT distinct
  *
FROM stg_glamira_raw__add_undefined_record
