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

# ### Understanding V-Order

# CELL ********************

spark.conf.get('spark.sql.parquet.vorder.enabled')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Reading NYC Green data into dataframe

# CELL ********************

df = spark.read.format('parquet').load('Files/NYDestinationCP')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Disable V-Order

# CELL ********************

spark.conf.set('spark.sql.parquet.vorder.enabled','false')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.get('spark.sql.parquet.vorder.enabled')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Writing data as table

# CELL ********************

df.write.format('delta').saveAsTable('delta_green_no_vorder')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Enabling V-Order

# CELL ********************

spark.conf.set('spark.sql.parquet.vorder.enabled','true')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.get('spark.sql.parquet.vorder.enabled')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Writing table with v-order

# CELL ********************

df.write.format('delta').saveAsTable('delta_green_vorder')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
