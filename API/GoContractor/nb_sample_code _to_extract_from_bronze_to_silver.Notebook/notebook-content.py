# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

#### NOTE:
#### This code is not tested at all.
#### This is code that is used by Keepmoat to extract HR data.
#### The code uses a Watermark table which it loops through to extract specific table data.
#### It is a dynamic way of looping though each table, rather than having a Notebook for each table we would want to extract from.
#### We could use this code to move from Bronze Staging to Bronze. In LS_Lakehouse, a watermark table does not exist for this mini project.

# Import the deltatable library to complete a merge directly against the delta table
from delta.tables import DeltaTable

# Read the watermark table columns into a dataframe for the load
load_silver_tables_meta = spark.sql(
    """select bronze_table,silver_table,merge_key,bronze_schema,silver_schema,watermark_column,silver_watermark_value
    from watermark_table where include_in_load = 1 and load_type='incremental'"""
).collect()

# List to store failed table retries
failed_tables = []

# Function to perform the merge operation
def perform_merge(table):

    # Extract table details
    Bronze_table_name = table["bronze_table"]
    Silver_table_name = table["silver_table"]
    business_key = table["merge_key"]
    bronze_schema = table["bronze_schema"]
    silver_schema = table["silver_schema"]
    incremental_load_field = table["watermark_column"]
    incremental_load_value = table["silver_watermark_value"]

    # Define source and target paths
    source_path = f"Tables/{bronze_schema}/{Bronze_table_name}"
    target_path = f"Tables/{silver_schema}/{Silver_table_name.lower()}"

    # Filter row based on the incremental load condition - use rank to remove historical records in bronze.
    filtered_source_df = spark.sql(
        f"""
        SELECT * FROM (
        SELECT *, RANK() OVER (PARTITION BY {business_key} ORDER BY {incremental_load_field} DESC) AS LATEST
        FROM {bronze_schema}.{Bronze_table_name} WHERE ({incremental_load_field}) > ({incremental_load_value}) )LATESTVALS
        WHERE LATEST =1
        """
    )

    # remove the latest column that was used for the rank to remove duplicate values
    filtered_source_df = filtered_source_df.drop("LATEST")

    print(f"Merge Attempt for: {Silver_table_name}")

    # Check if the filtered_source_df has rows; if there are no rows do not attempt a merge.
    filtered_source_count = filtered_source_df.count()
    if filtered_source_count > 0:
        print(f"rows available for merging: {filtered_source_count} for table: {Silver_table_name}")

        # Load the target table
        target_table = DeltaTable.forPath(spark, target_path)
        
        # Perform the merge statement
        target_table.alias("target").merge(
            filtered_source_df.alias("source"),
            f"target.{business_key} = source.{business_key}"
        ).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

        # Query the filtered source data for the max incremental load value
        max_incremental_value = filtered_source_df.selectExpr(f"MAX({incremental_load_field})").collect()[0][0]
    
    
        # Update the load_silver_tables table with the max incremental load value
        spark.sql(
            f"""
            UPDATE watermark_table
            SET silver_watermark_value = '{max_incremental_value}'
            WHERE silver_table = '{Silver_table_name}'
            """
        )

        print(f"INCREMENTAL_LOAD_VALUE updated to: {max_incremental_value} for table: {Silver_table_name}")

        else:
            print(f"No rows found in filtered_source_df for table: {Silver_table_name}. Skipping update.")
        except Exception as e:
        print(f"Error occurred for table {Silver_table_name}: {str(e)}")
        failed_tables.append(table)  # Add table to retry list

    # First pass: Attempt merge for all tables
    for table in load_silver_tables_meta:
        perform_merge(table)

    # Retry logic for failed tables
    retry_attempts = 1
    retry_delay = 10  # seconds

    for attempt in range(1, retry_attempts + 1):
        print(f"Retry attempt {attempt} for failed tables...")
        if not failed_tables:
            print("No failed tables to retry. Exiting retry loop.")
            break

        retry_list = failed_tables.copy()  # Copy failed tables to retry list
        failed_tables = []  # Clear for next attempt

        for table in retry_list:
            time.sleep(retry_delay)  # Delay before retrying
            perform_merge(table)

    if failed_tables:
        print(f"Some tables could not be processed after retries: {[table['Silver_table_name'] for table in failed_tables]}")
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
