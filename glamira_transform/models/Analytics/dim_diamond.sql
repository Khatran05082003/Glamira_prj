WITH dim_diamond AS (
    SELECT 
        DISTINCT
        cart.option
    FROM {{ ref('stg_raw') }} AS stg_raw, 
        UNNEST(stg_raw.cart_products) AS cart
)

SELECT 
    DISTINCT
    IFNULL(option.value_label_diamond, 'Unknown') AS diamond_value,
    CASE 
                WHEN option.value_label_diamond IS NULL THEN FARM_FINGERPRINT('Unknown')
                ELSE FARM_FINGERPRINT(option.value_label_diamond)
    END AS diamond_key
FROM 
    dim_diamond,
    UNNEST(dim_diamond.option) AS option  

UNION ALL

SELECT 
    DISTINCT
    'Unknown' AS diamond_value,
    FARM_FINGERPRINT('Unknown') AS diamond_key  
FROM 
    dim_diamond
WHERE 
    NOT EXISTS (
        SELECT 1
        FROM UNNEST(dim_diamond.option) AS option
    )
