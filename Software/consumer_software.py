from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, from_json, col, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField, TimestampType
from datetime import datetime
import json
from dotenv import load_dotenv
from os import getenv

load_dotenv()
mongo_conn_string = getenv("MONGO_CONN_STRING")

def create_spark_session():
    print("Creating Spark session")
    spark = SparkSession \
        .builder \
        .appName("KafkaToMongoDBWithSpark") \
        .config("spark.mongodb.output.uri", mongo_conn_string + "/g2hack.unavailableProducts") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .master("spark://986bff55b4d4:7077") \
        .getOrCreate()
    return spark

def main():
    spark = create_spark_session()
    
    # Schema for Kafka data
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("description", StringType(), True)
    ])
    print("Schema created")

    # Create DataFrame representing the stream of input from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "Software") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json")

    # Apply schema and add timestamp
    df = df.select(from_json(col("json"), schema).alias("data")).select("data.*")
    df = df.withColumn("timestamp", current_timestamp())

    # Windowed aggregation to count messages per 10 second window
    windowedCounts = df.groupBy(
        expr("window(timestamp, '10 seconds')")
    ).count()

    # Write the counts to the console for debugging
    query_count = windowedCounts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Write detailed records to MongoDB
    query_data = df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda batch_df, batch_id: batch_df.write.format("mongo").mode("append").save()) \
        .start()

    query_count.awaitTermination()
    query_data.awaitTermination()

    print("Execution complete", datetime.now())

if __name__ == "__main__":
    main()
