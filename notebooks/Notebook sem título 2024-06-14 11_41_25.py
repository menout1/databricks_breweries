# Databricks notebook source
import requests
import json

# Brewery API
link = 'https://api.openbrewerydb.org/breweries'

# Request data from link as 'str'
data = requests.get(link).text

# convert 'str' to Json
data = json.loads(data)

# COMMAND ----------

data

# COMMAND ----------


