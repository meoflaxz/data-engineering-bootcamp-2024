import os
import logging
import traceback
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session

# Set up logging configuration
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# destination of data
def create_events_sinks_postgres(t_env):
    try:
        table_name = 'processed_events_web_traffic'
        sink_ddl = f"""
                        CREATE TABLE {table_name} (
                            event_hour TIMESTAMP(3),
                            ip VARCHAR,
                            host VARCHAR,
                            url VARCHAR,
                            num_hits BIGINT
                        ) WITH (
                            'connector' = 'jdbc',
                            'url' = '{os.environ.get("POSTGRES_URL", "jdbc:postgresql://localhost:5432/postgres")}',
                            'table-name' = '{table_name}',
                            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
                            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
                            'driver' = 'org.postgresql.Driver'
                        );
                    """
        t_env.execute_sql(sink_ddl)
        logging.info("Postgres sink table created successfully.")
        return table_name
    except Exception as e:
        logging.error(f"Failed to create Postgres sink table: {str(e)}")
        logging.error("Error Details: %s", traceback.format_exc())
        raise

# creating kafka events as log data from website
def create_events_source_kafka(t_env):
    try:
        kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
        kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
        table_name = "process_events_kafka"
        pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
        source_ddl = f"""
                        CREATE TABLE {table_name} (
                            ip VARCHAR,
                            event_time VARCHAR,
                            referrer VARCHAR,
                            host VARCHAR,
                            url VARCHAR,
                            geodata VARCHAR,
                            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
                            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
                        ) WITH (
                            'connector' = 'kafka',
                            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
                            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
                            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
                            'properties.security.protocol' = 'SASL_SSL',
                            'properties.sasl.mechanism' = 'PLAIN',
                            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
                            'scan.startup.mode' = 'latest-offset',
                            'properties.auto.offset.reset' = 'latest',
                            'format' = 'json'
                        );
                    """
        t_env.execute_sql(source_ddl)
        logging.info("Kafka source table created successfully.")
        return table_name
    except Exception as e:
        logging.error(f"Failed to create Kafka source table: {str(e)}")
        logging.error("Error Details: %s", traceback.format_exc())
        raise

# create Flink job from Kafka inserting to Postgres
def log_traffic_event():
    try:
        logging.info("Starting Flink job...")

        env = StreamExecutionEnvironment.get_execution_environment()
        # every 10s, Flink will trigger a checkpoint to save application state
        env.enable_checkpointing(10 * 1000)
        env.set_parallelism(3)

        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        t_env = StreamTableEnvironment.create(env, environment_settings=settings)

        # running the kafka source table first
        source_table = create_events_source_kafka(t_env)
        # running the postgres sink table
        table_sink = create_events_sinks_postgres(t_env)

        t_env.from_path(source_table) \
            .window(
                Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("w")
            ).group_by(
                col("w"),
                col("ip"),
                col("host"),
                col("url"),
            ).select(
                col("w").start.alias("event_hour"),
                col("ip"),
                col("host"),
                col("url"),
                col("url").count.alias("num_hits")
            ).execute_insert(table_sink)

        logging.info("Flink job executed successfully.")
    except Exception as e:
        logging.error("Writing records from Kafka to Postgres failed")
        logging.error(f"Error: {str(e)}")
        logging.error("Error Details: %s", traceback.format_exc())
        raise

# running the event chain
if __name__ == '__main__':
    try:
        log_traffic_event()
    except Exception as e:
        logging.error(f"Job failed with error: {str(e)}")
        logging.error("Error Details: %s", traceback.format_exc())
