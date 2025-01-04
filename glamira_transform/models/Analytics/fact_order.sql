WITH fact_order AS (
  SELECT 
      GENERATE_UUID() AS order_line_key, 
      stg_raw.ip AS location_key,
      stg_raw.full_date AS order_date,
      stg_raw.collection AS collection,
      ARRAY(
        SELECT AS STRUCT
          cart_products.price,
          cart_products.amount,
          cart_products.currency,
          cart_products.product_id,
          ARRAY(
            SELECT FARM_FINGERPRINT(COALESCE(option.value_label_diamond, 'Unknown'))
            FROM UNNEST(cart_products.option) AS option
            WHERE option.value_label_diamond IS NOT NULL OR option.value_label_diamond = 'Unknown'
            LIMIT 1
          ) AS diamond_id,
          ARRAY(
            SELECT CASE 
                      WHEN STRPOS(option.value_label_alloy, '&') > 0 THEN SUBSTR(option.value_label_alloy, 1, STRPOS(option.value_label_alloy, '&') - 1)
                      ELSE option.value_label_alloy
                   END AS alloy_value
            FROM UNNEST(cart_products.option) AS option
          ) AS alloy_value
        FROM UNNEST(cart_products) AS cart_products
      ) AS cart_products
  FROM `main-cocoa-445214-r4.models_stg.stg_raw` AS stg_raw
)

SELECT 
  order_line_key,
  location_key,
  order_date,
  collection,
  cart_products[SAFE_OFFSET(0)].price AS price,
  cart_products[SAFE_OFFSET(0)].amount AS amount,
  cart_products[SAFE_OFFSET(0)].currency AS currency,
  cart_products[SAFE_OFFSET(0)].product_id AS product_key,
  cart_products[SAFE_OFFSET(0)].diamond_id AS diamond_key,
  CASE 
    WHEN 'Stainless Steel / 585 White / Red Gold' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white_red-edelstahl585']
    WHEN 'Weiß-Gelbgold 750' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white_yellow-750']
    WHEN 'Weiß-Rotgold 375' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white_red-375']
    WHEN 'Rotgold 750' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['red-750']
    WHEN 'Ceramic / 585 White Gold' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white-ceramic585']
    WHEN 'Gelb-Weißgold 585' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['yellow_white-585']
    WHEN 'Gelbgold 375' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['yellow-375']
    WHEN 'Rot-Weißgold 750' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['red_white-750']
    WHEN '950 Platin' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white-platin']
    WHEN 'Weißgold 750' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white-750']
    WHEN 'Weiß-Rotgold 585' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white_red-585']
    WHEN 'Ceramic / 585 Black / White Gold' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['black_white-ceramic585']
    WHEN '585 Natural White Gold' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white-585']
WHEN 'Stainless Steel / 585 White / Yellow Gold' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white_yellow-edelstahl585']
    WHEN 'Rot-Weißgold 585' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['red_white-585']
    WHEN 'Gelb-Weißgold 375' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['yellow_white-375']
    WHEN 'Gelbgold 750' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['yellow-750']
    WHEN '925 Silber' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white-silber']
    WHEN 'Rotgold 585' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['red-585']
    WHEN 'Rotgold 375' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['red-375']
    WHEN 'Weiß-Gelbgold 375' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white_yellow-375']
    WHEN 'Stainless Steel / 585 White Gold' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white-edelstahl585']
    WHEN 'Weiß-Gelb-Rotgold 585' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white_yellow_red-585']
    WHEN 'Weißgold 585' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white-585']
    WHEN '950 Palladium' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white-palladium']
    WHEN 'Gelb-Weißgold 750' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['yellow_white-750']
    WHEN 'Weißgold 375' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white-375']
    WHEN 'Weiß-Gelbgold 585' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white_yellow-585']
    WHEN 'Weiß-Gelb-Rotgold 375' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white_yellow_red-375']
    WHEN 'Gelbgold 585' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['yellow-585']
    WHEN 'Weiß-Rotgold 750' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['white_red-750']
    WHEN '585 Natural Yellow Gold' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['yellow-585']
    WHEN 'Rot-Weißgold 375' IN UNNEST(cart_products[SAFE_OFFSET(0)].alloy_value) THEN ['red_white-375']
    ELSE ['Unkonwn']
  END AS alloy_key
FROM fact_order
WHERE collection = 'checkout_success'
