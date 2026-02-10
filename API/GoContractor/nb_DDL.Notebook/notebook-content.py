# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cde07ee3-3b2d-4924-a2da-d9ac5970b70e",
# META       "default_lakehouse_name": "LS_Lakehouse",
# META       "default_lakehouse_workspace_id": "8bc6ea08-fd30-4ca7-be7f-dc76aee17419",
# META       "known_lakehouses": [
# META         {
# META           "id": "cde07ee3-3b2d-4924-a2da-d9ac5970b70e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# PySpark in a Fabric notebook
DB = "LS_LakeHouse"   # exact lakehouse name
TABLE = "Account"     # table name (no dbo in Spark)

# Set to True if you also want to DROP the table after truncating
#DO_DROP_TABLE = True

spark.sql(f"USE LS_LakeHouse")


#### 1) SELECT *
#df = spark.sql(f"SELECT * FROM LS_LakeHouse.Account")
#print(f"Row count: {df.count()}")
#df.show(20, truncate=False)

#### 2) DELETE all rows (keep schema) — TRUNCATE is fastest
#spark.sql(f"TRUNCATE TABLE '{DB}'.'{TABLE}'")
#remaining = spark.table(f"'{DB}'.'{TABLE}'").count()
#print(f"Row count after truncate: {remaining}")

#### 3) DROP TABLE (removes the table & data files for managed Delta tables)
#if DO_DROP_TABLE:
#    spark.sql(f"DROP TABLE IF EXISTS `{DB}`.`{TABLE}`")
#    print(f"Dropped table {DB}.{TABLE}")

#spark.sql(f"DROP TABLE IF EXISTS LS_LakeHouse.worker_workercourses")
#spark.sql(f"DROP TABLE IF EXISTS LS_LakeHouse.worker_workergroups")
#spark.sql(f"DROP TABLE IF EXISTS LS_LakeHouse.worker_workerqualifications")
#spark.sql(f"DROP TABLE IF EXISTS LS_LakeHouse.worker_workers")
#spark.sql(f"DROP TABLE IF EXISTS LS_LakeHouse.worker_workeruserdata")
#spark.sql(f"DROP TABLE IF EXISTS LS_LakeHouse.worker_workers_wide")

spark.sql(f"TRUNCATE TABLE LS_LakeHouse.bronze_gocontractor_accounts")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE EXTENDED LS_LakeHouse.stg_gocontractor_accounts

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

import pyspark.sql.functions as F
from delta.tables import *

df = spark.sql("SELECT * from LS_LakeHouse.stg_gocontractor_accounts")
df = df.drop("LoadDate")
df.write.format("delta").mode("overwrite").saveAsTable("stg_gocontractor_accounts_1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

import pyspark.sql.functions as F
from delta.tables import *

df = spark.sql("SELECT *, current_timestamp() as LoadDate  from LS_LakeHouse.stg_gocontractor_accounts_2")
df.write.format("delta").mode("overwrite").saveAsTable("stg_gocontractor_accounts_3")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

import pyspark.sql.functions as F
from delta.tables import *

df = spark.sql("SELECT * from LS_LakeHouse.stg_gocontractor_accounts")
df = df.drop("LoadDate")
df.write.format("delta").mode("overwrite").saveAsTable("stg_gocontractor_accounts_1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
