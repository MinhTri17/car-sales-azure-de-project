# Databricks notebook source
# MAGIC %md
# MAGIC # Data reading

# COMMAND ----------

dbutils.widgets.text("p_ingestion_date","")
v_ingest_date = dbutils.widgets.get("p_ingestion_date")
# print(v_incre_flag)

# COMMAND ----------

df = spark.read.format("parquet") \
              .option("inferschema",True) \
              .load(f"abfss://bronze@cardeprojectdl.dfs.core.windows.net/rawdata/{v_ingest_date}")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data transformation

# COMMAND ----------

from pyspark.sql.functions import split,col,count,sum,lit
from pyspark.sql.types import StringType

# COMMAND ----------

df = df.withColumn('model_category',split(col("Model_ID"),'-')[0])

# COMMAND ----------

df.withColumn('Units_Sold', col('Units_Sold').cast(StringType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn('RevPerUnit',col('Revenue')/col('Units_Sold'))

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("ingestion_date", lit(v_ingest_date))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # AGGREGATION

# COMMAND ----------

display(df.groupBy("Year", 'BranchName').agg(sum('Units_Sold').alias('Total_Units')).sort('Year','Total_Units',ascending= [True,False]))

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing

# COMMAND ----------

# %sql
# DROP TABLE cars_catalog.silver.silver_table;

# COMMAND ----------

# if spark.catalog.tableExists("cars_catalog.silver.silver_table"):
#   silver_df = spark.read.format("parquet") \
#               .load(f"abfss://silver@cardeprojectdl.dfs.core.windows.net/carsales")
#   df = df.union(silver_df)

# COMMAND ----------

df.write.format("parquet") \
    .mode("append") \
    .option("path", 'abfss://silver@cardeprojectdl.dfs.core.windows.net/carsales') \
    .saveAsTable("cars_catalog.silver.silver_table")

# COMMAND ----------

df.display()

# COMMAND ----------

# df.write.format("parquet") \
#     .mode("overwrite") \
#     .option("path", 'abfss://silver@cardeprojectdl.dfs.core.windows.net/carsales') \
#     .save()

# COMMAND ----------

# from delta.tables import DeltaTable
# # spark.conf.set("spark.databricks.optimizer.dynamicPartitionPrueving", "true")

# if spark.catalog.tableExists(f"cars_catalog.silver.silver_table"):
#     # deltaTable = DeltaTable.forPath(spark, "cars_catalog.silver.silver_table")
#     # target_table = spark.read.format("")
#     target_table = spark.read.format("parquet") \
#               .load(f"abfss://silver@cardeprojectdl.dfs.core.windows.net/carsales")

#     target_table.alias("tgt").merge(
#             df.alias("src"),
#         "tgt.Branch_ID = src.Branch_ID and tgt.Dealer_ID = src.Dealer_ID and tgt.Model_ID = src.Model_ID and tgt.Date_ID = src.Date_ID"
#     ) \
#         .whenMatchedUpdateAll() \
#         .whenNotMatchedInsertAll() \
#         .execute()
# else:
#     df.write.mode("overwrite").format("parquet").option("path", 'abfss://silver@cardeprojectdl.dfs.core.windows.net/carsales').saveAsTable("cars_catalog.silver.silver_table")

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM PARQUET.`abfss://silver@cardeprojectdl.dfs.core.windows.net/carsales`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cars_catalog.silver.silver_table

# COMMAND ----------

# %sql
# DROP TABLE cars_catalog.silver.silver_table;