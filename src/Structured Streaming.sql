-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC    .table("books")
-- MAGIC    .createOrReplaceTempView("books_streaming_tmp_vw"))

-- COMMAND ----------

select * from books_streaming_tmp_vw

-- COMMAND ----------

select author, count(book_id) as total
from books_streaming_tmp_vw
group by author

-- COMMAND ----------

/* create temporary view to pass the streaming temporary view to 
dataframe API so it becomes also a streaming one */
Create or replace temp view author_counts_tmp_vw as (
     select author, count(book_id) as total_books
     from books_streaming_tmp_vw
     group by author
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.table("author_counts_tmp_vw")
-- MAGIC   .writeStream
-- MAGIC   .trigger(processingTime="4 seconds")
-- MAGIC   .outputMode("complete")
-- MAGIC   .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
-- MAGIC   .table("author_counts")
-- MAGIC  )

-- COMMAND ----------

select * from author_counts

-- COMMAND ----------

INSERT INTO books
values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
        ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
        ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)
