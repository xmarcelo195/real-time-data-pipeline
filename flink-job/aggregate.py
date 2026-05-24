from pyflink.table import EnvironmentSettings, TableEnvironment
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Flink environment
logger.info("Creating table environment")
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Define input table (Kafka source)
logger.info("Executing transactions table SQL")
table_env.execute_sql("""
    CREATE TABLE transactions (
        name STRING,
        email STRING,
        address STRING,
        `timestamp` STRING,
        country STRING,
        currency STRING,
        `value` DOUBLE,
        item_description STRING,
        transaction_id STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'test-topic',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'flink-aggregation-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
""")

# Define output table (Upsert Kafka sink)
logger.info("Executing aggregates table SQL")
table_env.execute_sql("""
    CREATE TABLE aggregates (
        data_apenas STRING,
        moeda STRING,
        pais STRING,
        quantidade_registros BIGINT,
        soma_value DOUBLE,
        PRIMARY KEY (data_apenas, moeda, pais) NOT ENFORCED
    ) WITH (
        'connector' = 'upsert-kafka',
        'topic' = 'aggregated-topic',
        'properties.bootstrap.servers' = 'kafka:9092',
        'key.format' = 'json',
        'value.format' = 'json'
    )
""")

# Use SQL to perform the aggregation and date extraction
logger.info("Executing aggregation SQL")
table_env.execute_sql("""
    INSERT INTO aggregates
    SELECT
        DATE_FORMAT(`timestamp`, 'yyyy-MM-dd') AS data_apenas,
        currency AS moeda,
        country AS pais,
        COUNT(*) AS quantidade_registros,
        SUM(`value`) AS soma_value
    FROM transactions
    GROUP BY
        DATE_FORMAT(`timestamp`, 'yyyy-MM-dd'),
        currency,
        country
""")

# Execute job
logger.info("Starting job execution")
table_env.execute("Transaction Aggregation")