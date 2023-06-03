# Databricks notebook source
# MAGIC %run ./Includes/Copy-Datasets/

# COMMAND ----------

(spark.readStream
   .table("books")
   .createOrReplaceTempView("books_streaming_tmp_vw"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_streaming_tmp_vw

# COMMAND ----------

# MAGIC %sql 
# MAGIC select author, count(book_id) as total
# MAGIC from books_streaming_tmp_vw
# MAGIC group by author

# COMMAND ----------

# MAGIC %sql
# MAGIC /* create temporary view to pass the streaming temporary view to 
# MAGIC dataframe API so it becomes also a streaming one */
# MAGIC Create or replace temp view author_counts_tmp_vw as (
# MAGIC      select author, count(book_id) as total_books
# MAGIC      from books_streaming_tmp_vw
# MAGIC      group by author
# MAGIC )

# COMMAND ----------

(spark.table("author_counts_tmp_vw")
  .writeStream
  .trigger(processingTime="4 seconds")
  .outputMode("complete")
  .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
  .table("author_counts")
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------


