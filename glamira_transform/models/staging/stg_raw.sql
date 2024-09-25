WITH RawData AS (
  SELECT
    IFNULL(viewing_product_id, 'Unknown') AS viewing_product_id,
    IFNULL(user_agent, 'Unknown') AS user_agent,
    IFNULL(collect_id, 'Unknown') AS collect_id,
    SAFE_CAST(IFNULL(product_id, -1) AS INT64) AS product_id,  
    IFNULL(collection, 'Unknown') AS collection,
    IFNULL(SAFE_CAST(REPLACE(REPLACE(price, ',', ''), '.', '.') AS FLOAT64), 0) AS price,
    IFNULL(currency, 'Unknown') AS currency,
    SAFE_CAST(IFNULL(amount, 0) AS INT64) AS amount,
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
    ARRAY(
            SELECT AS STRUCT
                NULLIF(option.alloy,"Unknown") as alloy,
                NULLIF(option.diamond,"Unknown") as diamond,
                NULLIF(option.option_label,"Unknown") as option_label,
                NULLIF(option.option_id,"Unknown") as option_id,
                NULLIF(option.value_label,"Unknown") as value_label,
                NULLIF(option.value_id,"Unknown") as value_id,
                NULLIF(option.quality,"Unknown") as quality,
                NULLIF(option.quality_label,"Unknown") as quality_label
            FROM UNNEST(option) AS option
        ) AS option
  FROM
    `glamira-prj.glamira_dataset.glamira_raw`
),

stg_glamira_raw__add_undefined_record AS (
  SELECT 
    viewing_product_id,
    user_agent,
    collect_id,
    product_id,  
    collection,
    price,
    currency,
    amount,
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
    ARRAY(
            SELECT AS STRUCT
                option.alloy,
                option.diamond,
                option.option_label,
                option.option_id AS option_id,
                option.value_label,
                option.value_id ,
                option.quality,
                option.quality_label
            FROM UNNEST(option) AS option
        ) AS option
  FROM RawData

  UNION ALL

  SELECT
    'Unknown' AS viewing_product_id,
    'Unknown' AS user_agent,
    'Unknown' AS collect_id,
    -1 AS product_id, 
    'Unknown' AS collection,
    0 AS price,
    'Unknown' AS currency,
    0 AS amount, 
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
    ) AS option
)

SELECT
  *
FROM stg_glamira_raw__add_undefined_record
WHERE collection = 'checkout_success'
