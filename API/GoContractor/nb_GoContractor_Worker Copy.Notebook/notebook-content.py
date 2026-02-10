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
TABLE_PREFIX = "Worker_"                       # optionally "Workers_" to namespace tables

min_ts = F.lit("1900-01-01 00:00:00").cast("timestamp")

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
        F.col("w.subcontractor.name").alias("subconName")
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
    F.col("grp.qualified").alias("groupQualified")
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
        F.col("q.qualified").alias("qualified")
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

#display(ud)

ud_base = ud.select(
    "workerGuid",
    F.get_json_object("ud_json", "$.name").alias("fieldName"),
    F.get_json_object("ud_json", "$.type").alias("fieldType"),
    F.get_json_object("ud_json", "$.requirementGuid").alias("requirementGuid"),
    F.when(F.to_timestamp(F.get_json_object("ud_json", "$.expiryDate")) < min_ts, min_ts).otherwise(F.to_timestamp(F.get_json_object("ud_json", "$.expiryDate"))).alias("expiryDate"),
    F.get_json_object("ud_json", "$.data").alias("dataRaw")   # raw JSON string of the 'data' field (or a primitive string)
)

display(ud_base)

# Helper extracts from dataRaw (same as before)
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
        F.get_json_object("dataRaw", "$.testTime").cast("int").alias("testTimeSeconds")
    )
)

# ========= 7) WRITE DELTA TABLES =========
def save_tbl(df, name):
    tbl = f"{TABLE_PREFIX}{name}"
    (
        df.write
          .mode("overwrite")
          .format("delta")
          .option("overwriteSchema", "true")
          .saveAsTable(tbl)
    )
    print(f"Saved table: {tbl} (rows={df.count()})")

save_tbl(df_workers, "Workers")
save_tbl(df_groups, "WorkerGroups")
save_tbl(df_quals, "WorkerQualifications")
save_tbl(df_userdata, "WorkerUserData")
save_tbl(df_courses, "WorkerCourses")

# ========= 8) (Optional) A narrow "wide-ish" reporting view for common fields =========
# Example: pivot a few frequently-used userData fields (safe, selective pivot)

#import re

#def _sanitize_colname(c: str) -> str:
#    c = c.strip()                               # remove leading/trailing spaces
#    c = re.sub(r'\s+', '_', c)                  # spaces -> underscores
#    c = re.sub(r'[^A-Za-z0-9_]', '_', c)        # anything not alnum/_ -> _
#    c = re.sub(r'_+', '_', c)                   # collapse multiple _
#    if re.match(r'^\d', c):                     # cannot start with a digit
#        c = '_' + c
#    return c

#def sanitize_df_columns(df, keep=("workerGuid",)):
#    mapping = {}
#    seen = set(keep)
#    for col in df.columns:
#        if col in keep:
#            continue
#        new = _sanitize_colname(col)
#        base = new
#        i = 1
#        while new in seen:                      # avoid accidental collisions
#            i += 1
#            new = f"{base}_{i}"
#        if new != col:
#            df = df.withColumnRenamed(col, new)
#            mapping[col] = new
#        seen.add(new)
#    if mapping:
#        print("Renamed columns:", mapping)
#    return df


# OPTIONAL (recommended): trim field names for the pivot source so you don't have to keep trailing spaces
#df_ud_for_pivot = df_userdata.withColumn("fieldName", F.trim("fieldName"))

#COMMON_FIELDS = [
#    "Surname","First Name","Gender","Date Of Birth","Telephone Number","Home Postcode",
#    "Contact Name","Relationship to you","Contact Number","Name of Employer","Occupation / Job Title"
#]

#df_ud_common = (
#    df_ud_for_pivot
#      .filter(F.col("fieldName").isin(COMMON_FIELDS))
#      .groupBy("workerGuid")
#      .pivot("fieldName", COMMON_FIELDS)
#      .agg(F.first("dataValue", ignorenulls=True))
#)

# Sanitize pivoted column names so Delta accepts them
#df_ud_common = sanitize_df_columns(df_ud_common, keep=("workerGuid",))

#df_workers_wide = df_workers.join(df_ud_common, on="workerGuid", how="left")

# (extra safe) sanitize the final DF too, in case future fields slip through
#df_workers_wide = sanitize_df_columns(df_workers_wide, keep=("workerGuid",))

#save_tbl(df_workers_wide, "Workers_Wide")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
