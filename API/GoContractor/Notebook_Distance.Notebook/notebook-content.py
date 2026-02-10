# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import os
import requests
import json
from math import radians, sin, cos, sqrt, atan2
from notebookutils import mssparkutils   # or just import notebookutils

# --- Step 1: Get parameters from Fabric ---
#postcode1 = 'SW1A 1AA'
#postcode2 = 'OX1 2JD'
#api_key   = 'AIzaSyDCjvuVQGZFeA5fgYOGFjWWApuzFWT4N74'

# --- Step 2: Geocoding helper ---
def geocode(address, key):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": key}
    r = requests.get(url, params=params)
    r.raise_for_status()
    result = r.json()
    if result["status"] != "OK":
        raise ValueError(f"Geocoding failed for {address}: {result['status']}")
    loc = result["results"][0]["geometry"]["location"]
    return loc["lat"], loc["lng"]

# --- Step 3: Haversine formula ---
def haversine(lat1, lon1, lat2, lon2):
    #R = 6371.0  # Earth radius in km
    R = 3958.8  # Earth radius in miles
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c

# --- Step 4: Run calculation ---
lat1, lon1 = geocode(postcode1, api_key)
lat2, lon2 = geocode(postcode2, api_key)

distance_miles = haversine(lat1, lon1, lat2, lon2)


# --- Step 5: Return as pipeline output ---
from notebookutils import mssparkutils
result = {
    "Postcode1": postcode1,
    "Postcode2": postcode2,
    "DistanceMiles":  round(distance_miles)
}

print(result)
mssparkutils.notebook.exit(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
