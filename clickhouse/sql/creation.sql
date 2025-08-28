CREATE TABLE kafka.kafka__astros_data
(
    id UInt32,
    craft String,
    name String,
    _inserted_at UInt64
)ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
kafka_topic_list = 'postgres.public.astros_data',
kafka_group_name = 'clickhouse',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url='http://schema-registry:8081';

CREATE TABLE kafka.astros_data
(
    id UInt32,
    craft String,
    name String,
    _inserted_at DateTime,
    kafka_time Nullable(DateTime),
    kafka_offset UInt64
)ENGINE = ReplacingMergeTree(_inserted_at)
ORDER BY (craft, name);

CREATE MATERIALIZED VIEW kafka.astros_mv TO kafka.astros_data
(
    user_id UInt32,
    username String,
    account_type String,
    updated_at DateTime,
    created_at DateTime,
    kafka_time Nullable(DateTime),
    kafka_offset UInt64
) AS
SELECT
    id,
    craft,
    name,
    toDateTime(_inserted_at/ 1000000) AS _inserted_at,
    _timestamp AS kafka_time,
    _offset AS kafka_offset
FROM kafka.kafka__astros_data;