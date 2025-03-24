
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("NPI_Ingestion").getOrCreate()
file_path = "/FileStore/tables/npidata.csv"

df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

npi_df = df.select(
    col("NPI").alias("npi_number"),
    col("Provider Organization Name (Legal Business Name)").alias("provider_name"),
    col("Provider First Name").alias("first_name"),
    col("Provider Last Name").alias("last_name"),
    col("Provider Business Mailing Address City Name").alias("city"),
    col("Provider Business Mailing Address State Name").alias("state"),
    col("Healthcare Provider Taxonomy Code_1").alias("provider_type")
).filter(col("npi_number").isNotNull())

npi_df.write.format("delta").mode("overwrite").save("/mnt/delta/npi_data")
