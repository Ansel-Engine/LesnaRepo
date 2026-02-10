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

# MARKDOWN ********************

# ### External Table

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/Fabric - Files/*.csv")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 1 - Using .saveAsTable()

# CELL ********************

df.write.format('delta').saveAsTable('ext_empdelta',path='Files/Ext_empDeltatable')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED ext_empdelta

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 02. using SQL CREATE TABLE script

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE ext_empsql
# MAGIC USING DELTA
# MAGIC LOCATION 'Files/ext_sql'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dropping an external table

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DROP TABLE ext_empdelta

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
