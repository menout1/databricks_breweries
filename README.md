![image](https://github.com/menout1/databricks_breweries/assets/58555709/60b46ced-20f7-4e57-a4cf-0985626eb22c) - <h1> <b> Databricks_Breweries </b> </h1>

<h2> The Breweries Case </h2>

In order to do the the Breweries case, I used the Azure cloud provider which allow me to explore features of Databricks, DataFactory and Blob Storage Container  to load Breweries information from API. I also use Python, PySpark, Spark, and Parquet format with compress to save space in the cloud storage.

<b> It was created 3 layers Bronze, Silver and Gold in Azure Container </b>
![image](https://github.com/menout1/databricks_breweries/assets/58555709/1c0eb324-6d50-4ec6-b48a-fa33cab09134)

<b>And also was created 4 databricks notebooks on each one has its purpose.</b>
![image](https://github.com/menout1/databricks_breweries/assets/58555709/40687e08-5cf2-4d5b-a5aa-1a7e10414217)

<b> inbound_to_bronze - It gets the API Brewery in raw data and save into Bronze layer with json format file.</b>
![image](https://github.com/menout1/databricks_breweries/assets/58555709/53271bbc-a0c5-459b-a4f7-de3ec27ef710)

<b> bronze_to_silver - It transform the json file in PySpark dataframe, I also modify the data type of phone column to integer, in order to  avoid wrong values. In the end, it loads the file into the dataframe in parquet format partition by brewery location and gzip compress to save cloud storage.</b>

![image](https://github.com/menout1/databricks_breweries/assets/58555709/8efe0113-92e9-4a44-a6e4-2c7a87507fc1)

<b> silver_to_gold - This script gets data in the silver layer copied to gold layer. So, I created a view in order to do some aggregations queries.</b>

![image](https://github.com/menout1/databricks_breweries/assets/58555709/c370aa61-7a53-4f93-8131-5748a8435735)

<b> Another query and graph </b>
![image](https://github.com/menout1/databricks_breweries/assets/58555709/6b50297b-6ac7-4133-b3dd-38a68b8d912d)


<b> Azure Data Factory - It was used to build the pipeline, schedule the jobs and monitoring the results sending by email if it works with success or failed. </b> 

![image](https://github.com/menout1/databricks_breweries/assets/58555709/005412d8-d9d1-4234-959d-bdf33941a17c)

<b> I also created a trigger to run the job for each 1 hour. </b>

![image](https://github.com/menout1/databricks_breweries/assets/58555709/c53cb965-5237-42b9-b0d7-b45e71cd89f2)


<b>Email Success - </b>

![image](https://github.com/menout1/databricks_breweries/assets/58555709/817115e5-c88e-4b92-a1f2-2ce286bc6cd7)

<b>Email Failed -</b>

![image](https://github.com/menout1/databricks_breweries/assets/58555709/34b4343c-e34a-405a-bfc8-906a446ae857)

<b>Git Hub - All the scripts and datafacory objects was published in the git repository in the databricks_breweries branch </b>

![image](https://github.com/menout1/databricks_breweries/assets/58555709/1f365ba2-2771-4f0a-b713-56bb3a6250fa)









