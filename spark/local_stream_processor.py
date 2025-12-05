# spark/local_stream_processor.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType

def main(input_path="data/sample_stock.json", output_path="data/output_local"):
    spark = SparkSession.builder.master("local[*]").appName("LocalStockProcessor").getOrCreate()
    # read JSON; supports single JSON object or newline-delimited JSON
    df = spark.read.option("multiline","true").json(input_path)
    # sample input has fields: symbol, timestamp, raw (with nested Global Quote)
    # extract price from nested path
    df2 = df.withColumn("price_str", col("raw")["Global Quote"]["05. price"])
    df3 = df2.withColumn("price", col("price_str").cast("double"))
    df3 = df3.withColumn("event_time", col("timestamp").cast("timestamp"))
    # show results and write small parquet
    df3.select("symbol","event_time","price").show(truncate=False)
    df3.coalesce(1).write.mode("overwrite").parquet(output_path)
    print("Wrote output to", output_path)
    spark.stop()

if __name__ == "__main__":
    inp = sys.argv[1] if len(sys.argv)>1 else "data/sample_stock.json"
    out = sys.argv[2] if len(sys.argv)>2 else "data/output_local"
    main(inp,out)
