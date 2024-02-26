-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets

-- COMMAND ----------

select order_id, multiple_copies from (
select order_id, filter(books, b -> b.quantity >= 2) As multiple_copies
from orders)
where size(multiple_copies) > 0; 

-- COMMAND ----------

SELECT
  order_id,
  books,
  TRANSFORM (
    books,
    b -> CAST(b.subtotal * 0.8 AS INT)
  ) AS subtotal_after_discount
FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UDFs

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING

RETURN concat("https://www.", split(email, "@")[1])

-- COMMAND ----------

select * from customers

-- COMMAND ----------

select customer_id, get_url(email) as customer_site from customers

-- COMMAND ----------

describe function get_url

-- COMMAND ----------

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
          WHEN email like "%.com" THEN "Commercial business"
          WHEN email like "%.org" THEN "Non-profits organization"
          WHEN email like "%.edu" THEN "Educational institution"
          ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
       END;

-- COMMAND ----------

SELECT email, site_type(email) as domain_category
FROM customers

-- COMMAND ----------

drop function get_url;
drop function site_type
