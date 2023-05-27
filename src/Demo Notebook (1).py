# Databricks notebook source
print("hello world")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/databricks-datasets"

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets")
display(files)

# COMMAND ----------


