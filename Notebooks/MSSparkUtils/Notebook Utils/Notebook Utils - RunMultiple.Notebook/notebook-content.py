# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

mssparkutils.notebook.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.help('runMultiple')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run multiple notebooks in parallel

# CELL ********************

mssparkutils.notebook.runMultiple(['Notebook Utils- Child', "Notebook Utils- Child parameter"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run multiple notebooks  One after another

# CELL ********************

DAG = {
    "activities": [
        {
            "name": "Child parameter", # activity name, must be unique
            "path": "Notebook Utils- Child parameter", # notebook path
            "timeoutPerCellInSeconds": 90, # max timeout for each cell, default to 90 seconds
            "args": {"a": 50 , "b": 60}, # notebook parameters
            "workspace": "Fabric_trail", # workspace name, default to current workspace
            "retry": 0, # max retry times, default to 0
            "retryIntervalInSeconds": 0, # retry interval, default to 0 seconds
        },
        {
            "name": "Notebook child", #activity name
            "path": "Notebook Utils- Child",
            "timeoutPerCellInSeconds": 120,
            "args": {
                "useRootDefaultLakehouse": True, # set useRootDefaultLakehouse as True
            },
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "dependencies": ["Child parameter"]
        }
    ],
    "timeoutInSeconds": 43200, # max timeout for the entire pipeline, default to 12 hours
    "concurrency": 50 # max number of notebooks to run concurrently, default to 50, 0 means unlimited
}
notebookutils.notebook.runMultiple(DAG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
