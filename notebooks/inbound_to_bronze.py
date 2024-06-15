# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##GET API JSON - BREWERIES

# COMMAND ----------

import requests
import json

# Brewery API
link = 'https://api.openbrewerydb.org/breweries'

# Request data from link 
response= requests.get(link)

if response.status_code ==200:
    data=json.loads(response.text)
else:
    print (f"Error: {response.status_code}")



# COMMAND ----------

json_string = json.dumps(data)

# COMMAND ----------

json_string

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Write the data into the Bronze Layer

# COMMAND ----------

dbutils.fs.put('/mnt/dados/bronze/dataset_breweries/breweries.json', json_string, overwrite=True)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dados/bronze/dataset_breweries'))

# COMMAND ----------


