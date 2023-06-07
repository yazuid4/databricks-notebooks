-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets/

-- COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
display(files)

-- COMMAND ----------

select * from json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

select * from csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

CREATE TABLE book_csv 
    (book_id string, title string, author string, category string, price double)
using CSV
options (
    header = true, 
    delimiter = ";"
)
location "${dataset.bookstore}/books-csv"

-- COMMAND ----------

select * from book_csv

-- COMMAND ----------

describe extended book_csv

-- COMMAND ----------

(spark.read
.table("book_csv")
.write
.mode("append")
.format("csv")\
    .option("header", "true")
    .option("delimiter", ";")
    .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

select count(*) from book_csv

-- COMMAND ----------

refresh table book_csv

-- COMMAND ----------

create table customers as 
select * from json.`${dataset.bookstore}/customers-json`;

describe extended customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CTAS limitation with CSV (no options)

-- COMMAND ----------

/* temp view solution */
create temp view book_tmp
  (book_id string, title string, author string, category string, price double)
 using CSV
 options(
    path = "${dataset.bookstore}/books-csv/export_*.csv",
    header = "true",
    delimiter = ";"
 );



-- COMMAND ----------

create table books_new As
select * from book_tmp;

select * from books_new;

-- COMMAND ----------


