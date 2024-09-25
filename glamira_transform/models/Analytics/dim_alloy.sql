WITH stg_color_source AS (
  SELECT
    *
  FROM
    `glamira-prj.glamira_dataset.glamira_raw`
),
stg_color__code_value AS (
  SELECT
    DISTINCT
    option.value_label AS alloy_value,
    CASE
      WHEN STRPOS(option.value_label, '-') > 0 THEN SUBSTR(option.value_label, 1, STRPOS(option.value_label, '-') - 1)
      ELSE option.value_label
    END AS raw_color_name
  FROM
    stg_color_source,
    UNNEST(option) AS option
  WHERE
    option.option_label = 'alloy'
),
stg_metal_source AS (
  SELECT
    *
  FROM
    `glamira-prj.glamira_dataset.glamira_raw`
),
stg_metal__code_value AS (
  SELECT
    DISTINCT
    option.value_label AS alloy_value,
    CASE
      WHEN STRPOS(option.value_label, '-') > 0 THEN SUBSTR(option.value_label, STRPOS(option.value_label, '-') + 1)
      ELSE option.value_label
    END AS metal_code
  FROM
    stg_metal_source,
    UNNEST(option) AS option
  WHERE
    option.option_label = 'alloy'
),
metal_names AS (
  SELECT
    DISTINCT
    metal_code,
    CASE
      WHEN metal_code LIKE '%edelstahl%' THEN 'Edelstahl / ' || REGEXP_EXTRACT(metal_code, r'(\d{3})') || ' Gold'
      WHEN metal_code LIKE '%stainless%' THEN 'Stainless Steel / 14K Gold'
      WHEN metal_code LIKE '%750%' THEN '18K Gold - 750'
      WHEN metal_code LIKE '%417%' THEN '10K Gold - 417'
      WHEN metal_code LIKE '%platin_750%' THEN '750 Platinum'
      WHEN metal_code LIKE '%platin%' THEN '950 Platinum'
      WHEN metal_code LIKE '%silber_375%' THEN '375 Silver'
      WHEN metal_code LIKE '%platin_375%' THEN '375 Platinum'
      WHEN metal_code LIKE '%palladium%' THEN '950 Palladium'
      WHEN metal_code LIKE '%585%' THEN '14K Gold - 585'
      WHEN metal_code LIKE '%silber_417%' THEN '417 Silver'
      WHEN metal_code LIKE '%ceramic585%' THEN 'Ceramic / 585 Gold'
      WHEN metal_code LIKE '%platin_417%' THEN '417 Platinum'
      WHEN metal_code LIKE '%platin_585%' THEN '585 Platinum'
      WHEN metal_code LIKE '%silber%' THEN '925 Silver'
      WHEN metal_code LIKE '%375%' THEN '9K Gold - 375'
      ELSE 'Unknown Metal'
    END AS metal_name
  FROM
    stg_metal__code_value
)

SELECT 
  stg_color__code_value.alloy_value,
  ABS(FARM_FINGERPRINT(stg_color__code_value.raw_color_name)) AS color_key,
  CASE 
    WHEN LOWER(REPLACE(stg_color__code_value.raw_color_name, '_', ' ')) = 'red' THEN 'Rose'
    ELSE INITCAP(REPLACE(stg_color__code_value.raw_color_name, '_', ' '))
  END AS color_name,
  metal_names.metal_name,
  ABS(FARM_FINGERPRINT(metal_names.metal_name)) AS metal_key
FROM
  stg_color__code_value
JOIN
  stg_metal__code_value
ON
  stg_color__code_value.alloy_value = stg_metal__code_value.alloy_value
JOIN
  metal_names
ON
  stg_metal__code_value.metal_code = metal_names.metal_code
WHERE 
  TRIM(stg_color__code_value.raw_color_name) <> ''
  AND TRIM(stg_metal__code_value.alloy_value) <> ''
