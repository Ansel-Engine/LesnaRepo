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

# ## Managed Table

# CELL ********************

spark.catalog.listTables()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1 - Using .saveAsTable() function

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/Fabric - Files/*.csv")
# df now is a Spark DataFrame containing CSV data from "Files/Fabric - Files/Emp1.csv".


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Writing as delta format - managed

# CELL ********************

df.write.format('delta').option('header','true').mode('overwrite').saveAsTable('empsaveastable_delta')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED empsaveastable_delta

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Writing as CSV format - managed

# CELL ********************

df.write.format('csv').option('header','true').mode('overwrite').saveAsTable('empsaveastable_csv')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED empsaveastable_csv

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 02. Managed table using SQL create Table

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE emp_sql_delta
# MAGIC (
# MAGIC     Id INT NOT NULL,
# MAGIC     Age INT,
# MAGIC     Dep VARCHAR(20)
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED emp_sql_delta

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 03. Delta table builder API

# CELL ********************

from delta.tables import *

(DeltaTable.create(spark) 
    .tableName('Emp_delta_builder')
    .addColumn('Id','INT')
    .addColumn('Age','INT')
    .addColumn('Dep','INT')
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED Emp_delta_builder

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dropping a managed table

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DROP TABLE emp_sql_delta

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
