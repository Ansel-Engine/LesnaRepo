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

from pyspark.sql import SparkSession
from pyspark.sql.functions import round, to_timestamp, current_timestamp, current_date, date_format
from datetime import datetime



###################################################
# Define the path to the JSON file
#json_path = "abfss://Fabric_trail@onelake.dfs.fabric.microsoft.com/LH_Fabric.Lakehouse/Files/Fabric - Files/Countries.json"
#json_path = "Files/Fabric - Files/Countries.json"

# Read JSON file into DataFrame
#df = spark.read.json(json_path)
###################################################



def read_in_todays_data():
  '''
  Function that dynamically calculates the filepath based on todays date
  Returns: dataframe of today's data
  '''
  dt_now = datetime.now()
  display(dt_now) # 2025, 6, 2, 11, 56, 15, 963271

  dt_string = dt_now.strftime("%Y/%m/%d")
  display(dt_string) # 2025/06/02

  dynamic_file_path = f"Files/{dt_string}/Countries.json"

  try:
    df = spark.read.json(json_dynamic_file_pathpath)
    return df
  except Exception as err:
    print(err)


def convert_unix_to_datetime(unix_datetime_col):
  return date_format(unix_datetime_col, "yyyy-MM-dd HH:mm:ss")

def convert_area_2dp(area_col):
  return round(area_col)

df_selected = df.select(
  df["name.common"].alias("country_name"),
  df["name.official"].alias("official_country_name"),
  df["capital"].alias("capital"),
  df["flags.alt"].alias("flag_description"),
  convert_area_2dp(df["area"]).alias("area"), # this calls the function above (this makes debugging easier, as you can pass values in check as it goes along, rather than have a large select here (not quite sure how I call the function :( )))
  convert_unix_to_datetime(current_timestamp()).alias("load_date")
  )


# Optional: Inspect the result
#display(df_selected.printSchema())
display(df_selected)

# Write to Lakehouse table 'spanishspeakingcountries'
#df_selected.write.mode("overwrite").saveAsTable("spanishspeakingcountries") #I used this statement to initially create the Lakehouse table
df_selected.write.format("delta").mode("append").save("Tables/spanishspeakingcountries")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
