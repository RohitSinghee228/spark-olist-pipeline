from pyspark.sql import SparkSession
from src.transforms import normalize_text_columns

def main(input_csv: str, output_parquet: str):
    spark = SparkSession.builder.appName("olist-example-job").getOrCreate()
    df = spark.read.option("header", "true").csv(input_csv)
    df2 = normalize_text_columns(df, ["customer_city", "customer_state"])
    df2.write.mode("overwrite").parquet(output_parquet)
    spark.stop()

if __name__ == "__main__":
    import sys
    input_csv = sys.argv[1]
    output_parquet = sys.argv[2]
    main(input_csv, output_parquet)
