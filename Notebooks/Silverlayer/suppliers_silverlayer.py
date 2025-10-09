from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when,to_timestamp
from pyspark.sql.types import TimestampType




# Initialize Spark Session
spark = SparkSession.builder.appName("silver").getOrCreate()

# Google Cloud Storage (GCS) Configuration variables
GCS_BUCKET = "retailer-datalake-demo"
bronze_path=f"gs://{GCS_BUCKET}/landing/supplier-db/suppliers/*.json"
silver_path=f"gs://{GCS_BUCKET}/silver/suppliers/"


print("▶️ Processing Table: suppliers fullload")

# Read bronze data
bronze_df = spark.read.json(bronze_path)

#print("Bronze columns:", bronze_df.columns) 

bronze_df = (
    bronze_df
    .withColumn("created_at", to_timestamp((col("created_at") / 1000).cast("double")))
)


# Step 2: Prepare source data with additional columns
bronze_transformed = (
    bronze_df
    .withColumn(
        "is_quarantined",
        when(col("supplier_id").isNull() | col("supplier_name").isNull(), lit(True)).otherwise(lit(False))
    )
    .withColumn("effective_start_date", current_timestamp())
    .withColumn("effective_end_date", current_timestamp())
    .withColumn("is_active", lit(True))
)


bronze_transformed.write.mode("overwrite").parquet(silver_path)


print("▶️ Silver layer table suppliers saved")