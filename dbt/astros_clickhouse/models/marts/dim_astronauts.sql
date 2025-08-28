{{
    config(
        materialized='table',
        schema='marts',
        order_by='name'
    )
}}

WITH astronaut_stats AS (
    SELECT
        craft,
        name,
        min(_inserted_at) as first_seen,
        max(_inserted_at) as last_seen,
        count(*) as total_appearances,
        now() as current_timestamp
    FROM {{ source('raw_astros', 'astros_data') }}
    GROUP BY craft, name
),
current_astronauts AS (
    SELECT
        craft,
        name,
        valid_from
    FROM {{ ref('stg_astros') }}
    WHERE valid_to IS NULL
)
SELECT
    s.craft,
    s.name,
    s.first_seen,
    s.last_seen,
    s.total_appearances,
    CASE 
        WHEN c.name IS NOT NULL THEN 1 
        ELSE 0 
    END as is_currently_in_space,
    s.current_timestamp as dbt_updated_at
FROM astronaut_stats s
INNER JOIN current_astronauts c ON s.craft = c.craft AND s.name = c.name