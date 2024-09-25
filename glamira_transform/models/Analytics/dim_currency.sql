WITH currency_conversion AS (
    SELECT 
        currency,
        CASE 
            WHEN currency = 'CHF' THEN 1.04
            WHEN currency = 'CAD $' THEN 0.75
            WHEN currency = 'zł' THEN 0.25
            WHEN currency = 'CLP' THEN 0.0013
            WHEN currency = 'CRC ₡' THEN 0.0018
            WHEN currency = 'NZD $' THEN 0.61
            WHEN currency = 'Lei' THEN 0.23
            WHEN currency = 'лв.' THEN 0.56
            WHEN currency = 'PEN S/.' THEN 0.27
            WHEN currency = '₺' THEN 0.11
            WHEN currency = 'GTQ Q' THEN 0.13
            WHEN currency = '₱' THEN 0.020
            WHEN currency = '₫' THEN 0.000042
            WHEN currency = 'din.' THEN 1.50
            WHEN currency = 'kn' THEN 0.16
            WHEN currency = 'HKD $' THEN 0.13
            WHEN currency = '￥' THEN 0.0070
            WHEN currency = 'د.ك.‏' THEN 3.27
            WHEN currency = 'USD $' THEN 1.00
            WHEN currency = 'COP $' THEN 0.00026
            WHEN currency = '₹' THEN 0.012
            WHEN currency = 'BOB Bs' THEN 0.14
            WHEN currency = 'UYU' THEN 0.025
            WHEN currency = 'DOP $' THEN 0.018
            WHEN currency = 'R$' THEN 0.20
            WHEN currency = '₲' THEN 0.00014
            WHEN currency = '€' THEN 1.10
            WHEN currency = '£' THEN 1.25
            WHEN currency = 'kr' THEN 0.10
            WHEN currency = 'AU $' THEN 0.70
            WHEN currency = 'SGD $' THEN 0.74
            WHEN currency = '$' THEN 1.00
            WHEN currency = 'Kč' THEN 0.046
            WHEN currency = 'Ft' THEN 0.0034
            WHEN currency = 'Unknown' THEN 0
            WHEN currency = 'MXN $' THEN 0.054
            ELSE NULL
        END AS usd_conversion_rate
    FROM (
        SELECT 'CHF' AS currency UNION ALL
        SELECT 'CAD $' UNION ALL
        SELECT 'zł' UNION ALL
        SELECT 'CLP' UNION ALL
        SELECT 'CRC ₡' UNION ALL
        SELECT 'NZD $' UNION ALL
        SELECT 'Lei' UNION ALL
        SELECT 'лв.' UNION ALL
        SELECT 'PEN S/.' UNION ALL
        SELECT '₺' UNION ALL
        SELECT 'GTQ Q' UNION ALL
        SELECT '₱' UNION ALL
        SELECT '₫' UNION ALL
        SELECT 'din.' UNION ALL
        SELECT 'kn' UNION ALL
        SELECT 'HKD $' UNION ALL
        SELECT '￥' UNION ALL
        SELECT 'د.ك.‏' UNION ALL
        SELECT 'USD $' UNION ALL
        SELECT 'COP $' UNION ALL
        SELECT '₹' UNION ALL
        SELECT 'BOB Bs' UNION ALL
        SELECT 'UYU' UNION ALL
        SELECT 'DOP $' UNION ALL
        SELECT 'R$' UNION ALL
        SELECT '₲' UNION ALL
        SELECT '€' UNION ALL
        SELECT '£' UNION ALL
        SELECT 'kr' UNION ALL
        SELECT 'AU $' UNION ALL
        SELECT 'SGD $' UNION ALL
        SELECT '$' UNION ALL
        SELECT 'Kč' UNION ALL
        SELECT 'Ft' UNION ALL
        SELECT 'Unknown' UNION ALL
        SELECT 'MXN $'
    ) AS currency_list
)
SELECT * 
FROM currency_conversion
ORDER BY currency
