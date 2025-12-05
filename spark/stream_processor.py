# spark/stream_processor.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType

def main(output_path="data/output"):
    spark = SparkSession.builder \
        .appName("StockStreamProcessor") \
        .master("local[*]") \
        .getOrCreate()

    # schema expected for messages produced by fetch_stocks.py
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("raw", MapType(StringType(), StringType()), True)
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "stocks_raw") \
        .option("startingOffsets", "latest") \
        .load()

    # value is JSON string: parse it
    value_df = df.selectExpr("CAST(value AS STRING) as json_str")
    parsed = value_df.select(from_json(col("json_str"), schema).alias("data"))
    selected = parsed.select(
        col("data.symbol").alias("symbol"),
        col("data.timestamp").alias("ts"),
        col("data.raw").alias("raw")
    )

    # raw is a map where nested JSON may be string or map — attempt to extract Global Quote price
    # convert ts to timestamp
    out = selected.withColumn("event_time", to_timestamp(col("ts")))
    # write to parquet
    query = out.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", output_path + "/checkpoint") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "data/output"
    main(out)
