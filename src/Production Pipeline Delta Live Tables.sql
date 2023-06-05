-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Bronze Layer:

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
-- MAGIC COMMENT "The raw books orders, ingested from orders-raw"
-- MAGIC AS SELECT * FROM cloud_files("${datasets_path}/orders-json-raw", "json",
-- MAGIC                              map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE customers
COMMENT "The customers lookup table, ingested from customers-json"
AS SELECT * FROM json.`${datasets_path}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Silver Layer:

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned (
  CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned books orders with valid order_id"
AS
  SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books,
         c.profile:address:country as country
  FROM STREAM(LIVE.orders_raw) o
  LEFT JOIN LIVE.customers c
    ON o.customer_id = c.customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold Layer:

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
COMMENT "Daily number of books per customer in China"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.orders_cleaned
  WHERE country = "China"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


