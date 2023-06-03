# Databricks notebook source
# MAGIC %run ./Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_id, multiple_copies from (
# MAGIC select order_id, filter(books, b -> b.quantity >= 2) As multiple_copies
# MAGIC from orders)
# MAGIC where size(multiple_copies) > 0; 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   books,
# MAGIC   TRANSFORM (
# MAGIC     books,
# MAGIC     b -> CAST(b.subtotal * 0.8 AS INT)
# MAGIC   ) AS subtotal_after_discount
# MAGIC FROM orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ### UDFs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_url(email STRING)
# MAGIC RETURNS STRING
# MAGIC
# MAGIC RETURN concat("https://www.", split(email, "@")[1])

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, get_url(email) as customer_site from customers

# COMMAND ----------

# MAGIC %sql
# MAGIC describe function get_url

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION site_type(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC           WHEN email like "%.com" THEN "Commercial business"
# MAGIC           WHEN email like "%.org" THEN "Non-profits organization"
# MAGIC           WHEN email like "%.edu" THEN "Educational institution"
# MAGIC           ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
# MAGIC        END;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email, site_type(email) as domain_category
# MAGIC FROM customers

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop function get_url;
# MAGIC drop function site_type

# COMMAND ----------


