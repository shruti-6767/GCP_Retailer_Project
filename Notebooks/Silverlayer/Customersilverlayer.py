from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when, from_unixtime,to_timestamp
from pyspark.sql.types import TimestampType


# Initialize Spark Session
spark = SparkSession.builder.appName("silver").getOrCreate()

# Google Cloud Storage (GCS) Configuration variables
GCS_BUCKET = "retailer-datalake-demo"
bronze_path=f"gs://{GCS_BUCKET}/landing/retailer-db/customers/*.json"
silver_path=f"gs://{GCS_BUCKET}/silver/customers/"

print("▶️ Processing Table: Customer SCD2")

# Read bronze data
bronze_df = spark.read.json(bronze_path)

# bronze_df.show(5)
# bronze_df.schema


# Convert from  milliseconds to timestamp
bronze_df = bronze_df.withColumn("updated_at", to_timestamp((col("updated_at") / 1000).cast("double")))

# bronze_df.show(5)
# bronze_df.schema

# bronze_df.show(5)

# Step 2: Prepare source data with additional columns
bronze_transformed = (
    bronze_df
    .withColumn(
        "is_quarantined",
        when(col("customer_id").isNull() | col("email").isNull() | col("name").isNull(), lit(True)).otherwise(lit(False))
    )
    .withColumn("effective_start_date", current_timestamp())
    .withColumn("effective_end_date", current_timestamp())
    .withColumn("is_active", lit(True))
)

#bronze_transformed.show(5)

try:
    silver_df = spark.read.parquet(silver_path)
    active_silver_df = silver_df.filter(col("is_active") == True)
except Exception:
    active_silver_df = bronze_transformed.limit(0)
    
#active_silver_df.show(5)



# Step 3: Find records that changed (existing active records with updates)
changed_records = (
    active_silver_df.alias("t")
    .join(
        bronze_transformed.alias("s"),
        (col("t.customer_id") == col("s.customer_id")) & (col("t.is_active") == True),
        "inner"
    )
    .filter(
        (col("t.name") != col("s.name")) |
        (col("t.email") != col("s.email")) |
        (col("t.updated_at") != col("s.updated_at"))
    )
    .select("t.customer_id")
)

#changed_records.show(5)

silver_expired = (
    active_silver_df.alias("t")
    .join(changed_records, "customer_id", "leftsemi")
    .withColumn("is_active", lit(False))
    .withColumn("effective_end_date", current_timestamp())
)

#silver_expired.show(5)


# Keep unchanged records as-is
silver_unchanged = active_silver_df.subtract(silver_expired)

#silver_unchanged.show(5)

# Step 5: Get new/updated records from Bronze (insert if not exists or updated)
new_records = (
    bronze_transformed.alias("s")
    .join(
        active_silver_df.filter(col("is_active") == True).alias("t"),
        col("s.customer_id") == col("t.customer_id"),
        "leftanti"  # only new or updated
    )
)

#new_records.show(5)

# Step 6: Union everything for final Silver
final_silver = silver_unchanged.unionByName(silver_expired).unionByName(new_records)

#final_silver.show(5)


# Step 7: Write back to Silver
(final_silver.write
    .mode("overwrite")   # careful: overwrites the entire folder
    .parquet(silver_path)
)


print("▶️ Silver layer table customers saved")