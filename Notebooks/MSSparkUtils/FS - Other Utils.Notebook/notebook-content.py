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

mssparkutils.fs.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.ls('Files/')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Creating a folder 

# CELL ********************

mssparkutils.fs.mkdirs('Files/FileTest/')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Creating a file

# CELL ********************

mssparkutils.fs.put('Files/FileTest/test.csv','1,Test',True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.ls('Files/FileTest/')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## View content inside a file

# CELL ********************

mssparkutils.fs.head('Files/FileTest/test.csv')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Appending data in test.csv

# CELL ********************

file_path_append = 'Files/FileTest/test.csv'

mssparkutils.fs.append(file_path_append,'2,Testing2')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.head(file_path_append)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Copy file to one folder to other

# CELL ********************

source_path = "Files/FileTest/test.csv"
destination_path = "Files/CopiedFolder/test.csv"

mssparkutils.fs.cp(source_path, destination_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Move file from one folder to another

# CELL ********************

source_path = "Files/FileTest/test.csv"
destination_path_mv = "Files/MovedFolder/test.csv"

mssparkutils.fs.mv(source_path, destination_path_mv,True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Remove a folder

# CELL ********************

remove_path = 'Files/MovedFolder/'

mssparkutils.fs.rm(remove_path,True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

remove_path = 'Files/MovedFolder/'

mssparkutils.fs.exists(remove_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
