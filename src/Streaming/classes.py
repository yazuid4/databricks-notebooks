# Databricks notebook source
class ETLBatch:
    def __init__(self):
        self.base_dir = "/FileStore/streaming-spark/"
        self.table = "wordcount_table"

    def read_stream(self):
        lines = (spark.read.format('text')
                    .option("lineSep", ".")
                    .load(f"{self.base_dir}/data/sample"))
        return lines

    def prepare_and_process(self, df):
        from pyspark.sql.functions import lower, explode, \
                        trim, split, regexp_replace
        words = df.select(explode(split("value", " ")).alias("word"))
        word_procs = words.select(lower(trim("word")).alias("word"))
        processed = (word_procs.withColumn("word",
                        regexp_replace("word", "^[^a-z0-9]+|[^a-z0-9]+$", "").alias("word")))           
        return processed
    
    def get_wordcount(self, df):
        return df.groupby("word").count()

    def write_stream(self, df):
        (df.write.format("delta")
            .mode("overwrite")
            .saveAsTable(self.table))
        
    def WordCount(self):
        lines = self.read_stream()
        processed = self.prepare_and_process(lines)
        df_wordcount = self.get_wordcount(processed)
        self.write_stream(df_wordcount)

# COMMAND ----------

class ETLStream:
    def __init__(self):
        self.base_dir = "/FileStore/streaming-spark/"
        self.table = "wordcount_table"

    def read_stream(self):
        lines = (spark.readStream.format('text')
                    .option("lineSep", ".")
                    .load(f"{self.base_dir}/data/sample"))
        return lines

    def prepare_and_process(self, df):
        from pyspark.sql.functions import lower, explode, \
                        trim, split, regexp_replace
        words = df.select(explode(split("value", " ")).alias("word"))
        word_procs = words.select(lower(trim("word")).alias("word"))
        processed = (word_procs.withColumn("word",
                        regexp_replace("word", "^[^a-z0-9]+|[^a-z0-9]+$", "").alias("word")))           
        return processed
    
    def get_wordcount(self, df):
        return df.groupby("word").count()

    def write_stream(self, df):
        return (df.writeStream.format("delta")
                    .option("checkpointLocation", f"{self.base_dir}/checkpoint/wordcount/")
                    .outputMode("complete")
                    .toTable(self.table))
        
    def WordCount(self):
        lines = self.read_stream()
        processed = self.prepare_and_process(lines)
        df_wordcount = self.get_wordcount(processed)
        streamQuery = self.write_stream(df_wordcount)
        return streamQuery

# COMMAND ----------

class invoiceStream:
    def __init__(self):
        self.base_dir = "/FileStore/streaming-spark"
        self.table = "invoice_table"
    
    def read_stream(self):
        schema = """InvoiceNumber string, CreatedTime bigint, StoreID string,
                    PosID string, CashierID string, CustomerType string,
                    CustomerCardNo string, TotalAmount double, NumberOfItems bigint,
                    PaymentMethod string, TaxableAmount double, CGST double,
                    SGST double, CESS double, DeliveryType string,
                    DeliveryAddress struct<AddressLine string,City string,
                    ContactNumber string, PinCode string, State string>,
                    InvoiceLineItems array<struct<ItemCode string, ItemDescription string, 
                    ItemPrice double, ItemQty bigint, TotalValue double>>
                    """
        return (spark.readStream.
                    format("json").
                    schema(schema).
                    load(f"{self.base_dir}/data/landing_invoice"))
        
    def write_stream(self, df, trigger):
        query = (df.writeStream.
                    format("delta").
                    option("checkpointLocation", f"{self.base_dir}/checkpoint/invoice").
                    outputMode("append").
                    option("maxFilesPerTrigger", 1))
        if trigger == "batch":
            return query.trigger(availableNow=True).toTable(self.table)
        else:
            return query.trigger(processingTime=trigger).toTable(self.table)
    
    def prepare(self, df):
        from pyspark.sql.functions import expr
        df = (df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
                           "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City",
                           "DeliveryAddress.State","DeliveryAddress.PinCode", 
                           "explode(InvoiceLineItems) as LineItem"))
        return( df.withColumn("ItemCode", expr("LineItem.ItemCode"))
                        .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
                        .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
                        .withColumn("ItemQty", expr("LineItem.ItemQty"))
                        .withColumn("TotalValue", expr("LineItem.TotalValue"))
                        .drop("LineItem")
                )
        
    def processing(self, trigger):
        raw_invoices = self.read_stream()
        df_invoices = self.prepare(raw_invoices)
        Sdf = self.write_stream(df_invoices, trigger)
        return Sdf
