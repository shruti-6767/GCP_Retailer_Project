from pyspark.sql import SparkSession
from pyspark.sql.functions import col,  sum, countDistinct, expr,to_date

# Initialize Spark Session
spark = SparkSession.builder.appName("Gold").getOrCreate()


GCS_BUCKET = "retailer-datalake-demo"

orders_silver_path=f"gs://{GCS_BUCKET}/silver/orders/*.parquet"
order_items_silver_path=f"gs://{GCS_BUCKET}/silver/order_items/*.parquet"
products_silver_path=f"gs://{GCS_BUCKET}/silver/products/*.parquet"
categories_silver_path=f"gs://{GCS_BUCKET}/silver/categories/*.parquet"
temppath=f"gs://{GCS_BUCKET}/temp/"


print("▶️ Processing Sales Summary (sales_summary)")

# Read  data
orders_df = spark.read.parquet(orders_silver_path)
order_items_df = spark.read.parquet(order_items_silver_path)
products_df = spark.read.parquet(products_silver_path)
categories_df = spark.read.parquet(categories_silver_path)

products_df = products_df.withColumnRenamed("name", "product_name")
categories_df = categories_df.withColumnRenamed("name", "category_name")

# Convert timestamp to just the date (yyyy-MM-dd)
orders_df = orders_df.withColumn("order_date", to_date(col("order_date")))


df = (
    orders_df.alias("o")
    .join(order_items_df.alias("i"), col("o.order_id") == col("i.order_id"), "inner")
    .join(products_df.alias("p"), col("i.product_id") == col("p.product_id"), "inner")
    .join(categories_df.alias("c"), col("p.category_id") == col("c.category_id"), "inner")
    .filter(col("o.is_active") == True)
    .groupBy(
        col("o.order_date"),
        col("p.category_id"),
        col("c.category_name"),
        col("i.product_id"),
        col("p.product_name")
    )
    .agg(
        sum(col("i.quantity")).alias("total_units_sold"),
        expr("SUM(i.price * i.quantity)").alias("total_sales"),  # ✅ FIXED: use expr()
        countDistinct(col("o.customer_id")).alias("unique_customers")
    )
)


final_df = df.select( 
    col("order_date"), 
    col("category_id"), 
    col("category_name"), 
    col("product_id"), 
    col("product_name"), 
    col("total_units_sold"), 
    col("total_sales"), 
    col("unique_customers")
)


#final_df.show()
#final_df.schema


(final_df.write.format("bigquery") 
               .option("table", "myprojectudemy-471808.gold.sales_summary")
               .option("temporaryGcsBucket", "retailer-datalake-demo")
               .mode("append")
               .save())



