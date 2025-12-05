# pipeline/parquet_to_csv.py
import sys
from pyspark.sql import SparkSession

def main(parquet_path="data/output", csv_out="data/output_csv"):
    spark = SparkSession.builder.master("local[*]").appName("ParquetToCSV").getOrCreate()
    df = spark.read.parquet(parquet_path)
    df.coalesce(1).write.option("header","true").csv(csv_out)
    print("Wrote CSV to", csv_out)
    spark.stop()

if __name__ == "__main__":
    p = sys.argv[1] if len(sys.argv)>1 else "data/output"
    o = sys.argv[2] if len(sys.argv)>2 else "data/output_csv"
    main(p,o)
