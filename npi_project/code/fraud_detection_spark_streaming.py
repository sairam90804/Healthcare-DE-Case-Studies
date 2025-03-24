
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

schema = StructType([
    StructField("npi_number", StringType(), True),
    StructField("provider_type", StringType(), True),
    StructField("billed_amount", IntegerType(), True),
    StructField("service_code", StringType(), True)
])

claims_stream = spark.readStream.format("kafka")     .option("kafka.bootstrap.servers", "localhost:9092")     .option("subscribe", "claims_stream")     .load()     .selectExpr("CAST(value AS STRING)")     .selectExpr("from_json(value, 'npi_number STRING, provider_type STRING, billed_amount INT, service_code STRING') AS data")     .select("data.*")

fraudulent_claims = claims_stream.filter(col("billed_amount") > 10000)

fraudulent_claims.writeStream     .outputMode("append")     .format("console")     .start()     .awaitTermination()
