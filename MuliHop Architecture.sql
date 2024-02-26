-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets/

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC     .format("cloudFiles")
-- MAGIC     .option("cloudFiles.format", "parquet")
-- MAGIC     .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_raw")
-- MAGIC     .load(f"{dataset_bookstore}/orders-raw")
-- MAGIC     .createOrReplaceTempView("orders_raw_temp")
-- MAGIC         )

-- COMMAND ----------

create or replace temporary view orders_temp as (
   select *, current_timestamp() arrival_time, input_file_name() source_file
   from orders_raw_temp
)

-- COMMAND ----------

select * from orders_temp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # write stream to orders_bronze delta table
-- MAGIC (spark.table("orders_temp")
-- MAGIC  .writeStream
-- MAGIC  .format("delta")
-- MAGIC  .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_bronze")
-- MAGIC  .outputMode("append")
-- MAGIC  .table("orders_bronze")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC load_new_data()

-- COMMAND ----------

select count(*) from orders_bronze

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # table to join with the incoming data
-- MAGIC (spark.read
-- MAGIC       .format("json")
-- MAGIC       .load(f"{dataset_bookstore}/customers-json")
-- MAGIC       .createOrReplaceTempView("customers_lookup"))

-- COMMAND ----------

select * from customers_lookup limit 3

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC   .table("orders_bronze")
-- MAGIC   .createOrReplaceTempView("orders_bronze_temp"))

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
  SELECT order_id, quantity, o.customer_id, 
         c.profile:first_name as f_name,
         c.profile:last_name as l_name,
         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp,
         books
  FROM orders_bronze_temp o
  INNER JOIN customers_lookup c
  ON o.customer_id = c.customer_id
  WHERE quantity > 0)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # write results to orders_silver table
-- MAGIC (spark.table("orders_enriched_tmp")
-- MAGIC  .writeStream
-- MAGIC  .format("delta")
-- MAGIC  .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
-- MAGIC  .outputMode("append")
-- MAGIC  .table("orders_silver")
-- MAGIC )

-- COMMAND ----------

select count(*) from orders_silver

-- COMMAND ----------

select * from orders_silver

-- COMMAND ----------

-- MAGIC %python
-- MAGIC load_new_data()
