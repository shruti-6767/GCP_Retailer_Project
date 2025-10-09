from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when, from_unixtime,to_timestamp
from pyspark.sql.types import TimestampType




# Initialize Spark Session
spark = SparkSession.builder.appName("silver").getOrCreate()

# Google Cloud Storage (GCS) Configuration variables
GCS_BUCKET = "retailer-datalake-demo"
bronze_path=f"gs://{GCS_BUCKET}/landing/retailer-db/orders/*.json"
silver_path=f"gs://{GCS_BUCKET}/silver/orders/"

print("▶️ Processing Table: orders SCD2")

# Read bronze data
bronze_df = spark.read.json(bronze_path)

#print("Bronze columns:", bronze_df.columns) 

bronze_df = (
    bronze_df
    .withColumn("updated_at", to_timestamp((col("updated_at") / 1000).cast("double")))
    .withColumn("order_date", to_timestamp((col("order_date") / 1000).cast("double")))
)

# bronze_df.show(5)
# bronze_df.schema

# Step 2: Prepare source data with additional columns
bronze_transformed = (
    bronze_df
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
    
# active_silver_df.show(5)
# active_silver_df.schema



# # Step 3: Find records that changed (existing active records with updates)
changed_records = (
    active_silver_df.alias("t")
    .join(
        bronze_transformed.alias("s"),
        (col("t.order_id") == col("s.order_id")) & (col("t.is_active") == True),
        "inner"
    )
    .filter(
        (
            (col("t.customer_id") != col("s.customer_id")) |
            (col("t.order_date") != col("s.order_date")) |
            (col("t.total_amount") != col("s.total_amount")) |
            (col("t.updated_at") != col("s.updated_at"))
        )
    )
    .select("t.order_id")
)


# #changed_records.show(5)

silver_expired = (
    active_silver_df.alias("t")
    .join(changed_records, "order_id", "leftsemi")
    .withColumn("is_active", lit(False))
    .withColumn("effective_end_date", current_timestamp())
)

#silver_expired.show(5)


# Define the correct column order to match silver_expired
column_order = [
    "order_id",
    "customer_id",
    "order_date",
    "total_amount",
    "updated_at",
    "effective_start_date",
    "effective_end_date",
    "is_active"
]

# Reorder columns in both DataFrames
active_silver_df = active_silver_df.select(column_order)
silver_expired = silver_expired.select(column_order)


# Keep unchanged records as-is
silver_unchanged = active_silver_df.subtract(silver_expired)

# silver_unchanged.show(5)

# Step 5: Get new/updated records from Bronze (insert if not exists or updated)
new_records = (
    bronze_transformed.alias("s")
    .join(
        active_silver_df.filter(col("is_active") == True).alias("t"),
        col("s.is_active") == col("t.is_active"),
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

print("▶️ Silver layer table orders saved")