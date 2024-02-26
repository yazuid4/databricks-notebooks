# Databricks notebook source
# MAGIC %run ./classes

# COMMAND ----------

class ETLTest:
    def __init__(self):
        self.base_dir = "/FileStore/streaming-spark/"
        self.table = "wordcount_table"
    
    def purge_and_init(self):
        spark.sql(f"drop table if exists {self.table}")
        dbutils.fs.rm(f"/user/hive/warehouse/{self.table}", True)
        dbutils.fs.rm(f"{self.base_dir}/checkpoint/wordcount/", True)
        dbutils.fs.rm(f"{self.base_dir}/data/sample", True)
        dbutils.fs.mkdirs(f"{self.base_dir}/data/sample")

    def ingest_file(self, i):
        dbutils.fs.cp(f"FileStore/datasets/txt/text_data_{i}.txt",
        f"{self.base_dir}/data/sample")

    def assertions(self, expected):
        count = spark.sql(f"select sum(count) from {self.table}").collect()[0][0]
        assert count == expected, f"Test Failed, the current result is {count}"
        print("success!")

    def test_pipeline(self, expected):
        import time
        self.purge_and_init()
        etlstream = ETLStream()
        stream_query = etlstream.WordCount()
        ##
        self.ingest_file(1)
        print("waiting for 30 seconds...")
        time.sleep(30)
        self.ingest_file(2)
        print("waiting for 30 seconds...")
        time.sleep(30)
        self.ingest_file(3)
        ##
        time.sleep(30)
        stream_query.stop()
        self.assertions(expected)

# COMMAND ----------

class Test_stream_invoice:
    def __init__(self):
        self.base_dir = "/FileStore/streaming-spark/"
        self.table = "invoice_table"
    
    def purge_and_init(self):
        spark.sql(f"drop table if exists {self.table}")
        dbutils.fs.rm(f"/user/hive/warehouse/{self.table}", True)
        dbutils.fs.rm(f"{self.base_dir}/checkpoint/invoice/", True)
        dbutils.fs.rm(f"{self.base_dir}/data/landing_invoice", True)
        dbutils.fs.mkdirs(f"{self.base_dir}/data/landing_invoice")

    def ingest_file(self, i):
        dbutils.fs.cp(f"FileStore/datasets/json/invoice/invoices_{i}.json", f"{self.base_dir}/data/landing_invoice")

    def assertions(self, expected):
        count = spark.sql(f"select count(*) from {self.table}").collect()[0][0]
        assert count == expected, f"Test Failed, the current result is {count}"
        print("success!")

    def waitingProcessing(self, secs):
        import time
        print(f"waiting {secs} for processing...")
        time.sleep(secs)

    def test_pipeline(self, expected):
        self.purge_and_init()
        stream = invoiceStream()

        self.ingest_file(1)
        self.ingest_file(2)
        stream.processing("batch")
        self.waitingProcessing(5)
        self.assertions(expected[0])
        ##
        self.ingest_file(3)
        stream.processing("batch")
        self.waitingProcessing(5)
        self.assertions(expected[1])

# COMMAND ----------

st = Test_stream_invoice()
st.test_pipeline([2510, 3994])
