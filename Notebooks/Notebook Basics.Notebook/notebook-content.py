# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "537bc388-f6e5-4eca-8e61-134e0d7b8641",
# META       "default_lakehouse_name": "LH_Fabric",
# META       "default_lakehouse_workspace_id": "4c53cfdf-2d42-42a9-90ca-0fa58d140542"
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sc

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Drag and Drop tables and files of Lakehouse

# CELL ********************

df = spark.sql("SELECT * FROM LH_Fabric.employees LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%spark
# MAGIC 
# MAGIC val df = spark.sql("SELECT * FROM LH_Fabric.employees LIMIT 1000")
# MAGIC display(df)

# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/Fabric - Files/Emp1.csv")
# df now is a Spark DataFrame containing CSV data from "Files/Fabric - Files/Emp1.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import matplotlib.pyplot as plt
import matplotlib.image as mpimg
# Load image
image = mpimg.imread(f"{mssparkutils.nbResPath}/builtin/fabric.jpg")
# Let the axes disappear
plt.axis('off')
# Plot image in the output
image_plot = plt.imshow(image)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Collaborative development

# CELL ********************

print('Hey this is from Steve')

print('Hey this is Alice')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Built-in code snippets

# CELL ********************

delta_table_path = "Your delta table path" #fill in your delta table path 
data = spark.range(5,10) 
data.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession 
from pyspark.sql.types import * 

# Primary storage info 
account_name = 'Your primary storage account name' # fill in your primary account name 
container_name = 'Your container name' # fill in your container name 
relative_path = 'Your relative path' # fill in your relative folder path 

adls_path = 'abfss://%s@%s.dfs.core.windows.net/%s' % (container_name, account_name, relative_path) 
print('Primary storage account path: ' + adls_path) 

data = spark.range(5,10) 

# Write spark dataframe as a csv file 
csv_path = adls_path + 'Your file name ' 
data.write.csv(csv_path, mode = 'overwrite', header = 'true') 

# Write spark dataframe as a parquet file 
parquet_path = adls_path + ' Your file name ' 
data.write.parquet(parquet_path, mode = 'overwrite') 

# Write spark dataframe as a json file 
json_path = adls_path + 'Your file name ' 
data.write.json(json_path, mode = 'overwrite')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Magic Commands

# MARKDOWN ********************

# - %%pyspark - Python
# Execute a Python query against Spark Context.
# - %%spark = Scala
# Execute a Scala query against Spark Context.
# - %%sql SparkSQL -
# Execute a SparkSQL query against Spark Context.
# - %%html = Html
# Execute a HTML query against Spark Context.
# - %%sparkr = R
# Execute a R query against Spark Context.

# CELL ********************

# MAGIC %%sparkr
# MAGIC 
# MAGIC results <- sql("SELECT * FROM LH_Fabric.UnEmployment LIMIT 1000")
# MAGIC head(results)

# METADATA ********************

# META {
# META   "language": "r",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create custom magic commands

# CELL ********************

from IPython.core.magic import (register_line_magic,register_cell_magic,register_line_cell_magic)
@register_line_magic
def DisplayMyName(name):
    return "The entered name is : " + name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%DisplayMyName Shanmukh

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

%DisplayMyName Shanmukh

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# MARKDOWN ********************

# ### Drag and drop image from local computer

# MARKDOWN ********************

# ![fabric.jpg](attachment:e8e2b6b4-de2a-45b9-a10e-4438180a287c.jpg)

# ATTACHMENTS ********************

# ATTA {
# ATTA   "e8e2b6b4-de2a-45b9-a10e-4438180a287c.jpg": {
# ATTA     "image/jpeg": "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"
# ATTA   }
# ATTA }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
