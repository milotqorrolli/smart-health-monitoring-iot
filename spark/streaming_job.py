import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, to_timestamp, when
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "smart-health-data")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "smart_health")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "sensor_readings")


schema = StructType(
    [
        StructField("patient_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("heart_rate", IntegerType(), False),
        StructField("spo2", DoubleType(), False),
        StructField("temperature", DoubleType(), False),
        StructField("systolic_bp", IntegerType(), False),
        StructField("diastolic_bp", IntegerType(), False),
        StructField("respiratory_rate", IntegerType(), False),
    ]
)


def add_patient_status(df):
    emergency = (
        (col("heart_rate") < 40)
        | (col("heart_rate") > 150)
        | (col("spo2") < 85)
        | (col("temperature") >= 40.0)
        | (col("systolic_bp") > 200)
        | (col("diastolic_bp") > 130)
        | (col("respiratory_rate") < 8)
        | (col("respiratory_rate") > 35)
    )
    critical = (
        (col("heart_rate") < 50)
        | (col("heart_rate") > 130)
        | (col("spo2") < 90)
        | (col("temperature") >= 39.0)
        | (col("systolic_bp") > 180)
        | (col("diastolic_bp") > 120)
        | (col("respiratory_rate") < 10)
        | (col("respiratory_rate") > 30)
    )
    warning = (
        (col("heart_rate") < 60)
        | (col("heart_rate") > 100)
        | (col("spo2") < 95)
        | (col("temperature") < 36.0)
        | (col("temperature") > 37.8)
        | (col("systolic_bp") > 140)
        | (col("diastolic_bp") > 90)
        | (col("respiratory_rate") < 12)
        | (col("respiratory_rate") > 20)
    )

    return df.withColumn(
        "status",
        when(emergency, "EMERGENCY")
        .when(critical, "CRITICAL")
        .when(warning, "WARNING")
        .otherwise("NORMAL"),
    )


def write_batch_to_cassandra(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    (
        batch_df.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE)
        .save()
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("SmartHealthStructuredStreaming")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df.select(from_json(col("value").cast("string"), schema).alias("reading"))
        .select("reading.*")
        .withColumn("reading_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        .drop("timestamp")
    )

    output_df = add_patient_status(parsed_df).withColumn("processed_at", current_timestamp())

    query = (
        output_df.writeStream.foreachBatch(write_batch_to_cassandra)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/smart-health-checkpoint")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
