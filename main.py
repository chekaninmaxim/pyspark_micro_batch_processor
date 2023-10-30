from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def main():
    print("Real-Time Streaming Data Pipeline Started ...")

    KAFKA_TOPIC_NAME = "random_numbers"

    # In-container configuration:
    SPARK_MASTER_SERVER = "spark://spark-master:7077"
    KAFKA_BOOTSTRAP_SERVER = "kafka:9092"
    HIVE_METASTORE_URI = "thrift://hive-metastore:9083"

    HIVE_DATABASE = "test1"
    HIVE_TABLE = "batch_mean_and_size"

    # Local run configuration
    # SPARK_MASTER_SERVER = "local[*]"
    # KAFKA_BOOTSTRAP_SERVER = "localhost:9093"
    # HIVE_METASTORE_URI = "thrift://localhost:9083"

    processing_time = "5 seconds"

    spark = SparkSession.builder \
        .master(SPARK_MASTER_SERVER) \
        .appName("Real-Time Streaming Data Pipeline") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2') \
        .config("hive.metastore.uris", HIVE_METASTORE_URI) \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    messages = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of event_message_detail_df: ")
    messages.printSchema()

    messageSchema = StructType([
        StructField("id", StringType()),
        StructField("timestamp", StringType()),
        StructField("value", IntegerType())
    ])

    parsedMessageDf = messages \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json("value", messageSchema).alias("message_object")) \
        .select("message_object.*")

    print("Printing Schema of parsed messages: ")
    parsedMessageDf.printSchema()

    parsedMessageDf.writeStream \
        .trigger(processingTime=processing_time) \
        .outputMode("append") \
        .foreachBatch(lambda batch_df, batch_id: write_batch(
            batch_df,
            batch_id,
            "{}.{}".format(HIVE_DATABASE, HIVE_TABLE))
                      ) \
        .start()

    parsedMessageWriteStream = parsedMessageDf \
        .writeStream \
        .trigger(processingTime=processing_time) \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()

    parsedMessageWriteStream.awaitTermination()

    print("Real-Time Streaming Data Pipeline Completed.")


def write_batch(batch_df, batch_id, tbl_name):
    (batch_df.select(
        F.lit(batch_id).alias("batch_id"),
        F.avg("value").alias("batch_mean"),
        F.max("timestamp").alias("processing_timestamp"),
        F.count("value").alias("batch_size"))
     .write.mode("append")
     .format("hive")
     .saveAsTable(tbl_name))


if __name__ == "__main__":
    main()
