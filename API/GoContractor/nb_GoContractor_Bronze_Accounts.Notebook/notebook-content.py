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

# Create the table LS_Lakehouse.stg_GoContractor_Workers (one column: raw_record STRING)

TARGET_DB  = "LS_Lakehouse"
TARGET_TBL = "bronze_gocontractor_accounts"

# Ensure the LS_Lakehouse is attached to the notebook
dbs = [db.name for db in spark.catalog.listDatabases()]
if TARGET_DB not in dbs:
    raise RuntimeError(
        f"Lakehouse '{TARGET_DB}' is not attached. "
        "Attach it to this notebook (as Primary or Secondary) and re-run."
    )

spark.sql(f"USE {TARGET_DB}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TBL} (
 guid STRING,
 account_name STRING,
    logo_url  STRING,
    relationship STRING,
     LoadedDate TIMESTAMP NOT NULL

)
USING DELTA
""")

print(f"Table ready: {TARGET_DB}.{TARGET_TBL}")
spark.sql(f"DESCRIBE EXTENDED {TARGET_DB}.{TARGET_TBL}").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Fabric notebook cell (Python / PySpark)
# Map stg_gocontractor_accounts -> bronze_gocontractor_accounts via INSERT INTO

from pyspark.sql import functions as F

# ---------------- Config ----------------
DB         = "LS_Lakehouse"
SRC_TABLE  = p_SRC_TABLE #"stg_gocontractor_accounts"
DST_TABLE  = p_DST_TABLE #"bronze_gocontractor_accounts"

# --------------- Setup / checks ----------
dbs = [db.name for db in spark.catalog.listDatabases()]
if DB not in dbs:
    raise RuntimeError(f"Lakehouse '{DB}' is not attached. Attached: {dbs}")

spark.sql(f"USE {DB}")
src_full = f"{DB}.{SRC_TABLE}"
dst_full = f"{DB}.{DST_TABLE}"


# 1) Get the single LoadDate value from staging
row = spark.sql(f"""
  SELECT date_format(LoadDate, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS ts
  FROM {DB}.{SRC_TABLE}
  LIMIT 1
""").first()

ts = row.ts if row else None
print(f"Staging LoadDate: {ts}")

# 2) Delete bronze rows matching that LoadDate (only if present)
if ts is not None:
    to_del = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM {DB}.{DST_TABLE}
        WHERE LoadDate = to_timestamp('{ts}')
    """).first().cnt
    print(f"Rows matching LoadDate {ts} in {DST_TABLE}: {to_del}")

    if to_del > 0:
        spark.sql(f"""
            DELETE FROM {DB}.{DST_TABLE}
            WHERE LoadDate = to_timestamp('{ts}')
        """)
        print(f"Delete complete for LoadDate = {ts}")
    else:
        print("No matching rows to delete. Skipping delete.")
else:
    print("No LoadDate found in staging. Skipping delete logic.")

# 3) Read source and shape to target schema
df_src = spark.table(src_full)

df_out = df_src.select(
    F.col("guid").alias("guid"),
    F.col("name").alias("account_name"),
    F.col("logoUrl").alias("logo_url"),
    F.col("relationship").alias("relationship"),
    F.col("LoadDate").cast("timestamp").alias("LoadDate")
)

# 4) INSERT INTO existing table using explicit column list

# Sanity count before insert
total = df_out.count()
print(f"Preparing to INSERT {total} row(s) into {dst_full}")

df_out.createOrReplaceTempView("_append_accounts")

spark.sql(f"""
  INSERT INTO {DB}.{DST_TABLE} (guid, account_name, logo_url, relationship, LoadDate)
  SELECT guid, account_name, logo_url, relationship, LoadDate
  FROM _append_accounts
""")

print("Append completed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#from delta.tables import DeltaTable
#dt = DeltaTable.forName(spark, "LS_Lakehouse.bronze_gocontractor_accounts")
#dt.delete("true")   # delete all rows

spark.sql(f"DELETE FROM LS_Lakehouse.bronze_gocontractor_accounts")  ######LS remove this  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
