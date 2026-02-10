# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

i_origins = 'HA3 8UF'
i_destinations = 'MK40 2ED'
i_mode = 'driving'
i_units = 'imperial'
i_key = 'AIzaSyB94X0cLJ5AicsA63FTASdWVhD0LYgSJ5o'
o_distance = ''
o_duration = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

import requests


#param_origins = notebookParams["origins"]
##param_destinations = notebookParams["destinations"]

i_origins = i_origins
i_destinations = i_destinations
i_mode = i_mode
i_units = i_units
i_key = i_key


# Read parameters passed from pipeline


url = "https://maps.googleapis.com/maps/api/distancematrix/json"
params = {
    "origins": i_origins,
    "destinations": i_destinations,
    "mode": i_mode,
    "units": i_units,
    "key": i_key
}

response = requests.get(url, params=params)
data = response.json()

o_distance = data["rows"][0]["elements"][0]["distance"]["text"]
o_duration = data["rows"][0]["elements"][0]["duration"]["text"]
#print(o_distance)
#mssparkutils.notebook.exit(o_distance)
#mssparkutils.notebook.exit(o_duration)

#notebookSession.set_output("o_distance", o_distance)
#notebookSession.set_output("o_duration", o_duration)

result = {"Distance": o_distance, "Duration": o_duration}
print(result)
#mssparkutils.notebook.exit(result)
mssparkutils.notebook.exit(
     {
     "Distance": data["rows"][0]["elements"][0]["distance"]["text"],
     "Duration": data["rows"][0]["elements"][0]["duration"]["text"]
     }
    )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

