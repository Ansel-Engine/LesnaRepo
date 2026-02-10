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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### FS Utilities

# CELL ********************

mssparkutils.fs.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Mounting Azure datalake Storage

# CELL ********************

from notebookutils import mssparkutils  
# get access token for keyvault resource
# you can also use full audience here like https://vault.azure.net

accountKey = "<key>"
mssparkutils.fs.mount(  
    "abfss://shortcutfile@datalake11212.dfs.core.windows.net",  
    "/FileShortcutMount",  
    {"accountKey":accountKey}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.mounts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.mounts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.ls(f"file://{mssparkutils.fs.getMountPath('/FileShortcutMount')}/")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
