# Databricks notebook source
# MAGIC %run ./Includes/Copy-Datasets/

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`${dataset.bookstore}/customers-json/export_001.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`${dataset.bookstore}/books-csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE book_csv 
# MAGIC     (book_id string, title string, author string, category string, price double)
# MAGIC using CSV
# MAGIC options (
# MAGIC     header = true, 
# MAGIC     delimiter = ";"
# MAGIC )
# MAGIC location "${dataset.bookstore}/books-csv"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from book_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended book_csv

# COMMAND ----------

(spark.read
.table("book_csv")
.write
.mode("append")
.format("csv")\
    .option("header", "true")
    .option("delimiter", ";")
    .save(f"{dataset_bookstore}/books-csv"))

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from book_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table book_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC create table customers as 
# MAGIC select * from json.`${dataset.bookstore}/customers-json`;
# MAGIC
# MAGIC describe extended customers;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CTAS limitation with CSV (no options)

# COMMAND ----------

# MAGIC %sql
# MAGIC /* temp view solution */
# MAGIC create temp view book_tmp
# MAGIC   (book_id string, title string, author string, category string, price double)
# MAGIC  using CSV
# MAGIC  options(
# MAGIC     path = "${dataset.bookstore}/books-csv/export_*.csv",
# MAGIC     header = "true",
# MAGIC     delimiter = ";"
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table books_new As
# MAGIC select * from book_tmp;
# MAGIC
# MAGIC select * from books_new;

# COMMAND ----------


