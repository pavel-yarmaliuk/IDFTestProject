{{
    config(
        materialized='view',
        schema='staging'
    )
}}

WITH ranked_data AS (
    SELECT
        craft,
        name,
        _inserted_at,
        rowNumberInAllBlocks() as row_num
    FROM {{ source('raw_astros', 'astros_data') }}
    ORDER BY craft, name, _inserted_at DESC
),

deduplicated AS (
    SELECT
        craft,
        name,
        _inserted_at,
        row_num
    FROM ranked_data
    WHERE (craft, name, _inserted_at) IN (
        SELECT craft, name, max(_inserted_at)
        FROM ranked_data
        GROUP BY craft, name
    )
)

SELECT
    craft,
    name,
    _inserted_at as valid_from,
    null as valid_to,
    row_num
FROM deduplicated