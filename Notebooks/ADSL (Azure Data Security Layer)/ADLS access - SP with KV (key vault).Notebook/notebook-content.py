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

# MARKDOWN ********************

# #### ADLS Access using Service principal with keyvault

# CELL ********************

kv_name = 'kvfabricaccess'
kv_app_id_name = 'AppIDSecret'
kv_tenantID_name = 'TenantIDName'
kv_keyname = 'KeyName'
storageAccount = "datalake11212"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.credentials.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kv_appid = mssparkutils.credentials.getSecret(akvName='https://kvfabricaccess.vault.azure.net/',secret= kv_app_id_name )
kv_tenantID = mssparkutils.credentials.getSecret(akvName='https://kvfabricaccess.vault.azure.net/',secret=kv_tenantID_name )
kv_keyname = mssparkutils.credentials.getSecret(akvName='https://kvfabricaccess.vault.azure.net/',secret=kv_keyname )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set(f"fs.azure.account.auth.type.{storageAccount}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storageAccount}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storageAccount}.dfs.core.windows.net", kv_appid)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storageAccount}.dfs.core.windows.net", kv_keyname)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storageAccount}.dfs.core.windows.net", f"https://login.microsoftonline.com/{kv_tenantID}/oauth2/token")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

account_name = 'datalake11212'
container_name = 'shortcutfile'
relative_path = 'Emp/*.csv'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adls_path = 'abfss://%s@%s.dfs.core.windows.net/%s' % (container_name, account_name, relative_path)
df = spark.read.format('csv').option('header','true').load(adls_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write data to lakehouse table

# CELL ********************

df.write.format('delta').mode('overwrite').saveAsTable('EmpIngested')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LH_Fabric.empingested LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
