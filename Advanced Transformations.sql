-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets/

-- COMMAND ----------

select * from customers

-- COMMAND ----------

/* :  Spark SQL syntax to travers nested string data */ 
select profile:first_name, profile:last_name, email from customers

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_customers AS
  SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"Thomas",
  "last_name":"Lane","gender":"Male",
  "address":{"street":"06 Boulevard Victor Hugo",
  "city":"Paris","country":"France"}}')) AS profile_struct
  FROM customers;
  
SELECT * FROM parsed_customers

-- COMMAND ----------

select profile_struct.first_name from parsed_customers

-- COMMAND ----------

describe parsed_customers

-- COMMAND ----------

/* Dealing with arrays in Spark SQL */
select customer_id, quantity, total, books from orders

-- COMMAND ----------

select customer_id, explode(books) as books from orders

-- COMMAND ----------

select customer_id, order_id, books.book_id from orders

-- COMMAND ----------

select customer_id, collect_set(order_id), 
         array_distinct(flatten(collect_set(books.book_id))) from orders
group by customer_id

-- COMMAND ----------

/* Join example */
CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM (
  SELECT *, explode(books) AS book 
  FROM orders) o
INNER JOIN books b
ON o.book.book_id = b.book_id;

SELECT * FROM orders_enriched

-- COMMAND ----------

/* Pivot operation */
CREATE OR REPLACE TABLE transactions AS
SELECT * FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);
SELECT * FROM transactions


-- COMMAND ----------


