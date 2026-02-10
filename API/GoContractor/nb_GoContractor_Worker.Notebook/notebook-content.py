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

# Fabric PySpark notebook — flatten paged worker JSON into Delta tables
from pyspark.sql import functions as F, types as T

# ========= 0) CONFIG =========
INPUT_PATH   = "Files/GoContractor - Worker/Workers.json"   # set path in the attached Lakehouse
DB_NAME      = None                     # e.g. "lh.default"; None = use default
TABLE_PREFIX = "stg_worker_"            # optionally "Workers_" to namespace tables

# ========= 0a) TIMESTAMPS =========
min_ts = F.lit("1900-01-01 00:00:00").cast("timestamp")

captured_ts = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]
load_ts = F.lit(captured_ts).cast("timestamp")

# ========= 1) READ RAW JSON (handles pretty JSON or NDJSON) =========
df_pages = (spark.read
  .option("multiLine", False)      # supports pretty-printed JSON 
  .option("mode", "PERMISSIVE")
  .json(INPUT_PATH)
)

# Keep only objects that have a 'data' array i.e. one row per page
df_pages = df_pages.filter(F.col("data").isNotNull())

# Each file/page object has a 'data' array of workers i.e. explode to one row per worker object
df_w = df_pages.select(F.explode_outer("data").alias("w"))

# ========= 2) WORKERS (one row per worker) =========
df_workers = (
    df_w.select(
        F.col("w.guid").alias("workerGuid"),
        F.when(F.to_timestamp("w.acceptedDateTime") < min_ts, min_ts).otherwise(F.to_timestamp("w.acceptedDateTime")).alias("acceptedDateTime"),
        F.col("w.user.guid").alias("userGuid"),
        F.col("w.user.firstName").alias("firstName"),
        F.col("w.user.lastName").alias("lastName"),
        F.col("w.user.email").alias("email"),
        F.col("w.user.phone").alias("phone"),
        F.col("w.subcontractor.guid").alias("subconGuid"),
        F.col("w.subcontractor.name").alias("subconName"),
        load_ts.alias("LoadDate")
    )#.dropDuplicates(["workerGuid"]) LS: Removed dropDuplicates
)

# ========= 3) WORKER GROUPS (one row per membership, can support many) =========
df_groups = (
    df_w.select(
        F.col("w.guid").alias("workerGuid"),
        F.col("w.workerGroups").alias("groups")
    )
    .withColumn("grp", F.explode_outer("groups")).select(
    "workerGuid",
    F.col("grp.guid").alias("groupGuid"),
    F.col("grp.qualified").alias("groupQualified"),
    load_ts.alias("LoadDate")
    )
)

# ========= 4) QUALIFICATIONS (one row per qualification, can support many) =========
df_quals = (
    df_w.select(
        F.col("w.guid").alias("workerGuid"),
        F.col("w.qualifications").alias("qualifications")
    )
    .withColumn("q", F.explode_outer("qualifications"))
    .select(
        "workerGuid",
        F.col("q.guid").alias("qualificationGuid"),
        F.col("q.name").alias("qualificationName"),
        F.col("q.qualified").alias("qualified"),
        load_ts.alias("LoadDate")
    )
)

# ========= 5) USERDATA (robust: works even if userData inferred as array<map> or mixed) =========
# Explode to one element per row, then parse fields via JSON to avoid schema inference issues
# This includes all userData including the Course data.  Course data is extracted in the next section.
ud = (
    df_w
    .select(
        F.col("w.guid").alias("workerGuid"),
        F.explode_outer(F.col("w.userData")).alias("ud_elem")
    )
    .withColumn("ud_json", F.to_json(F.col("ud_elem")))
)

ud_base = ud.select(
    "workerGuid",
    F.get_json_object("ud_json", "$.name").alias("fieldName"),
    F.get_json_object("ud_json", "$.type").alias("fieldType"),
    F.get_json_object("ud_json", "$.requirementGuid").alias("requirementGuid"),
    F.when(F.to_timestamp(F.get_json_object("ud_json", "$.expiryDate")) < min_ts, min_ts).otherwise(F.to_timestamp(F.get_json_object("ud_json", "$.expiryDate"))).alias("expiryDate"),
    load_ts.alias("LoadDate"),
    F.get_json_object("ud_json", "$.data").alias("dataRaw")   # raw JSON string of the 'data' field (or a primitive string)
)

# Helper extracts from dataRaw (same as before)d
arr_str_t = T.ArrayType(T.StringType())
selected_opts_json = F.get_json_object(F.col("dataRaw"), "$.selectedOptions")
selected_opts_arr  = F.from_json(selected_opts_json, arr_str_t)
selected_opts_join = F.array_join(selected_opts_arr, ", ")

df_userdata = (
    ud_base
    .withColumn(
        "dataValue",
        F.coalesce(
            F.get_json_object("dataRaw", "$.value"),
            F.get_json_object("dataRaw", "$.freeInputValue"),
            selected_opts_join,
            # if dataRaw is actually a quoted primitive (e.g., Upload URL), strip quotes
            F.regexp_replace(F.col("dataRaw"), r'^\s*"(.*)"\s*$', r'\1')
        )
    )
)

# ========= 6) COURSES (subset of userData where fieldType='Course') =========
df_courses = (
   df_userdata.filter(F.col("fieldType") == F.lit("Course"))
    .select(
        "workerGuid",
        F.col("fieldName").alias("courseName"),
        "requirementGuid",     
        F.when(F.to_timestamp("expiryDate") < min_ts, min_ts).otherwise(F.to_timestamp("expiryDate")).alias("expiryDate"),
        F.get_json_object("dataRaw", "$.courseGuid").alias("courseGuid"),
        F.col("dataValue").alias("courseDisplayValue"),
        F.get_json_object("dataRaw", "$.generateCertificateUrl").alias("certificateUrl"),
        F.get_json_object("dataRaw", "$.isCompleted").cast("boolean").alias("isCompleted"),
        F.get_json_object("dataRaw", "$.isPassed").cast("boolean").alias("isPassed"),
        F.get_json_object("dataRaw", "$.testAttempts").cast("int").alias("testAttempts"),
        F.get_json_object("dataRaw", "$.testScore").cast("double").alias("testScore"),
        F.get_json_object("dataRaw", "$.passScore").cast("double").alias("passScore"),
        F.when(F.to_timestamp(F.get_json_object("dataRaw", "$.testDate")) < min_ts, min_ts).otherwise(F.to_timestamp(F.get_json_object("dataRaw", "$.testDate"))).alias("testDate"),
        F.get_json_object("dataRaw", "$.testTime").cast("int").alias("testTimeSeconds"),
        load_ts.alias("LoadDate"),
    )
)

# ========= 7) WRITE DELTA TABLES =========

# --- Workers ---
workers_cols = ["workerGuid","acceptedDateTime","userGuid","firstName","lastName","email","phone","subconGuid","subconName","LoadDate"]
df_workers.select(*workers_cols).createOrReplaceTempView("_ins_workers")

spark.sql(f"TRUNCATE TABLE {TABLE_PREFIX}Workers")
spark.sql(f"""
  INSERT INTO {TABLE_PREFIX}Workers (
    workerGuid, acceptedDateTime, userGuid, firstName, lastName, email, phone, subconGuid, subconName, LoadDate
  )
  SELECT
    workerGuid, acceptedDateTime, userGuid, firstName, lastName, email, phone, subconGuid, subconName, LoadDate
  FROM _ins_workers
""")
print("Inserted into Workers")

# --- WorkerGroups ---
groups_cols = ["workerGuid","groupGuid","groupQualified","LoadDate"]
df_groups.select(*groups_cols).createOrReplaceTempView("_ins_workergroups")

spark.sql(f"TRUNCATE TABLE {TABLE_PREFIX}WorkerGroups")
spark.sql(f"""
  INSERT INTO {TABLE_PREFIX}WorkerGroups (
    workerGuid, groupGuid, groupQualified, LoadDate
  )
  SELECT
    workerGuid, groupGuid, groupQualified, LoadDate
  FROM _ins_workergroups
""")
print("Inserted into WorkerGroups")

# --- WorkerQualifications ---
quals_cols = ["workerGuid","qualificationGuid","qualificationName","qualified","LoadDate"]
df_quals.select(*quals_cols).createOrReplaceTempView("_ins_workerquals")

spark.sql(f"TRUNCATE TABLE {TABLE_PREFIX}WorkerQualifications")
spark.sql(f"""
  INSERT INTO {TABLE_PREFIX}WorkerQualifications (
    workerGuid, qualificationGuid, qualificationName, qualified, LoadDate
  )
  SELECT
    workerGuid, qualificationGuid, qualificationName, qualified, LoadDate
  FROM _ins_workerquals
""")
print("Inserted into WorkerQualifications")

# --- WorkerUserData ---
userdata_cols = ["workerGuid","fieldName","fieldType","requirementGuid","expiryDate","LoadDate","dataRaw","dataValue"]
df_userdata.select(*userdata_cols).createOrReplaceTempView("_ins_userdata")

spark.sql(f"TRUNCATE TABLE {TABLE_PREFIX}WorkerUserData")
spark.sql(f"""
  INSERT INTO {TABLE_PREFIX}WorkerUserData (
    workerGuid, fieldName, fieldType, requirementGuid, expiryDate, LoadDate, dataRaw, dataValue
  )
  SELECT
    workerGuid, fieldName, fieldType, requirementGuid, expiryDate, LoadDate, dataRaw, dataValue
  FROM _ins_userdata
""")
print("Inserted into WorkerUserData")

# --- WorkerCourses ---
courses_cols = [
  "workerGuid","courseName","requirementGuid","expiryDate","courseGuid","courseDisplayValue",
  "certificateUrl","isCompleted","isPassed","testAttempts","testScore","passScore","testDate","testTimeSeconds","LoadDate"
]
df_courses.select(*courses_cols).createOrReplaceTempView("_ins_courses")

spark.sql(f"TRUNCATE TABLE {TABLE_PREFIX}WorkerCourses")
spark.sql(f"""
  INSERT INTO {TABLE_PREFIX}WorkerCourses (
    workerGuid, courseName, requirementGuid, expiryDate, courseGuid, courseDisplayValue,
    certificateUrl, isCompleted, isPassed, testAttempts, testScore, passScore, testDate, testTimeSeconds, LoadDate
  )
  SELECT
    workerGuid, courseName, requirementGuid, expiryDate, courseGuid, courseDisplayValue,
    certificateUrl, isCompleted, isPassed, testAttempts, testScore, passScore, testDate, testTimeSeconds, LoadDate
  FROM _ins_courses
""")
print("Inserted into WorkerCourses")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
