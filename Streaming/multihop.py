# Databricks notebook source
class BronzeLayer:
    def __init__(self):
        self.base_dir = "/FileStore/streaming-spark"
        self.table = "invoice_bronze_table"
    
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
                    option("cleanSource", "archive").
                    option("sourceArchiveDir", f"{self.base_dir}/data/invoices_archive").
                    load(f"{self.base_dir}/data/landing_invoice"))
    
    def process(self):
        df = self.read_stream()
        query = (df.writeStream.
                    queryName("bronze_ingestion").
                    format("delta").
                    option("checkpointLocation", f"{self.base_dir}/checkpoint/{self.table}").
                    outputMode("append").
                    toTable(self.table))
        return query

# COMMAND ----------

class SilverLayer:
    def __init__(self):
        self.base_dir = "/FileStore/streaming-spark"
        self.bronze_table = "invoice_bronze_table"
        self.silver_table = "invoice_silver_table"

    def read_stream(self):
        return spark.readStream.table(self.bronze_table)
    
    def prepare(self):
        from pyspark.sql.functions import expr
        df = self.read_stream()
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
    
    def process(self):
        df = self.prepare()
        query = (df.writeStream.
                    queryName("silver_processing").
                    format("delta").
                    option("checkpointLocation", f"{self.base_dir}/checkpoint/{self.silver_table}").
                    outputMode("append").
                    toTable(self.silver_table))
        return query


# COMMAND ----------

class Test:
    def __init__(self):
        self.base_dir = "/FileStore/streaming-spark"
        self.bronze_table = "invoice_bronze_table"
        self.silver_table = "invoice_silver_table"

    def purge_and_init(self):
        spark.sql(f"drop table if exists {self.bronze_table}")
        spark.sql(f"drop table if exists {self.silver_table}")
        dbutils.fs.rm(f"/user/hive/warehouse/{self.bronze_table}", True)
        dbutils.fs.rm(f"/user/hive/warehouse/{self.silver_table}", True)
        
        dbutils.fs.rm(f"{self.base_dir}/checkpoint/{self.bronze_table}", True)
        dbutils.fs.rm(f"{self.base_dir}/checkpoint/{self.silver_table}", True)

        dbutils.fs.rm(f"{self.base_dir}/data/invoices_archive", True)
        dbutils.fs.rm(f"{self.base_dir}/data/landing_invoice", True)
        dbutils.fs.mkdirs(f"{self.base_dir}/data/landing_invoice")

    def ingest_file(self, nb):
        dbutils.fs.cp(f"/FileStore/datasets/json/invoice/invoices_{nb}.json",\
             f"{self.base_dir}/data/landing_invoice")
    
    def waitingProcessing(self, ts):
        import time
        print(f"waiting {ts} for processing...")
        time.sleep(ts)
        
    def extract(self):
        query = spark.sql(f"select count(*) from {self.silver_table}")
        return query.collect()

    def run(self):
        self.purge_and_init()
        lz = BronzeLayer().process()
        sl = SilverLayer().process()

        self.ingest_file(1)
        self.waitingProcessing(10)
        self.ingest_file(2)
        self.waitingProcessing(10)

        results = self.extract()
        lz.stop()
        sl.stop()
        return results


# COMMAND ----------

results = Test().run()
print(results)

# COMMAND ----------


