-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE TABLE employees
-- MAGIC   (id INT, name STRING, salary DOUBLE);

-- COMMAND ----------

INSERT INTO employees
VALUES 
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5),
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3),
  (5, "Anna", 2500.0),
  (6, "Kim", 6200.3)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM employees

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

UPDATE employees 
SET salary = salary + 100
WHERE name LIKE "A%"

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Time Travel:

-- COMMAND ----------

Select * from employees
version  as of 1

-- COMMAND ----------

select * from employees@v3

-- COMMAND ----------

DELETE from employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

restore table employees to version as of 2

-- COMMAND ----------

select * from employees

-- COMMAND ----------

DESCRIBE history employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Optimization / Compacting:

-- COMMAND ----------

DESCRIBE detail employees 

-- COMMAND ----------

optimize employees 
zorder by id

-- COMMAND ----------


