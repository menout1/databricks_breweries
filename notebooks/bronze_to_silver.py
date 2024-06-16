# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Write JSON File to Silver Layer

# COMMAND ----------

#checking if the file is ready.
dbutils.fs.ls('/mnt/dados/bronze/dataset_breweries')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating dataframe and checking if the rows is ok.

# COMMAND ----------

df = spark.read.json('/mnt/dados/bronze/dataset_breweries/breweries.json')
display(df)

# COMMAND ----------

#checking the columns datatypes
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Modifying the phone column datatype from string to integer

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

df_new = df.withColumn ('phone', col('phone').cast(IntegerType()))

# COMMAND ----------

#Checking if the phone datatype was modified.
df_new.printSchema() 



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Working with parquet, also do the compress in order to save space in the container.

# COMMAND ----------

df_new.write \
    .mode("overwrite") \
    .option('compression','gzip') \
    .format('parquet') \
    .save('/mnt/dados/silver/dataset_breweries/parquet')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dados/silver/dataset_breweries/parquet'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Show the file in the silver layer

# COMMAND ----------

df_parquet = spark.read.format('parquet') \
    .load('/mnt/dados/silver/dataset_breweries/parquet', compression='gzip')

display(df_parquet)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Partition the parquet by city

# COMMAND ----------

df_parquet.write \
    .partitionBy ("city") \
    .mode('overwrite') \
    .parquet('/mnt/dados/silver/dataset_breweries/parquet')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dados/silver/dataset_breweries/parquet'))

# COMMAND ----------

df_denver = spark.read \
    .parquet('/mnt/dados/silver/dataset_breweries/parquet/city=Denver')

display(df_denver)

# COMMAND ----------


