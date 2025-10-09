
CREATE EXTERNAL TABLE IF NOT EXISTS `myprojectudemy-471808.silver.customers`
(
    customer_id INT64,
    name STRING,
    email STRING,
    updated_at DATETIME,
    is_quarantined BOOL,
    effective_start_date DATETIME,
    effective_end_date DATETIME,
    is_active BOOL
)
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://retailer-datalake-demo/silver/customers/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `myprojectudemy-471808.silver.orders`(
    order_id INT64,
    customer_id INT64,
    order_date DATETIME,
    total_amount FLOAT64,
    updated_at DATETIME,
    effective_start_date DATETIME,
    effective_end_date DATETIME,
    is_active BOOL
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://retailer-datalake-demo/silver/orders/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `myprojectudemy-471808.silver.order_items`
(
    order_item_id INT64,
    order_id INT64,
    product_id INT64,
    quantity INT64,
    price FLOAT64,
    updated_at DATETIME,
    effective_start_date DATETIME,
    effective_end_date DATETIME,
    is_active BOOL
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://retailer-datalake-demo/silver/order_items/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `myprojectudemy-471808.silver.products`
(
    product_id INT64,
    name STRING,
    category_id INT64,
    price FLOAT64,
    updated_at DATETIME,
    is_quarantined BOOL
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://retailer-datalake-demo/silver/products/*.parquet']
);


CREATE EXTERNAL TABLE IF NOT EXISTS `myprojectudemy-471808.silver.categories`
(
    category_id INT64,
    name STRING,
    updated_at DATETIME,
    is_quarantined BOOL
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://retailer-datalake-demo/silver/categories/*.parquet']
);

--------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS `myprojectudemy-471808.silver.product_suppliers`
(
    supplier_id INT64,
    product_id INT64,
    supply_price FLOAT64,
    last_updated DATETIME,
    effective_start_date DATETIME,
    effective_end_date DATETIME,
    is_active BOOL
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://retailer-datalake-demo/silver/product_suppliers/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `myprojectudemy-471808.silver.suppliers`
(
    supplier_id INT64,
    supplier_name STRING,
    contact_name STRING,
    phone STRING,
    email STRING,
    address STRING,
    city STRING,
    country STRING,
    created_at DATETIME
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://retailer-datalake-demo/silver/suppliers/*.parquet']
);

