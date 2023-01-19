# Databricks notebook source
ENV="devl"
tenantID = "83b0f5ea-6499-4e52-84e1-f586e318d865"

applicationId = dbutils.secrets.get(scope='scope0', key='databrickspnid')
authenticationKey = dbutils.secrets.get(scope='scope0', key='databrickspnsecret')
endpoint = "https://login.microsoftonline.com/" + tenantID + "/oauth2/token"
configs = {"fs.azure.account.auth.type": "OAuth",
	           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
	           "fs.azure.account.oauth2.client.id": applicationId,
	           "fs.azure.account.oauth2.client.secret": authenticationKey,
	           "fs.azure.account.oauth2.client.endpoint": endpoint}

# COMMAND ----------

#get the mount display string
def get_mount_point(mountName):
  return '/mnt/' + mountName

# COMMAND ----------

# get the mount source(target) string. foldername is optional
def get_source(adlsContainerName, adlsAccountName, folderName):
  if folderName != None: # check null or empty
    source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + folderName 
  else:
    source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/"
  return source

# COMMAND ----------

# Mount only if directory is not mounted
def create_mount(mountName, adlsContainerName, adlsAccountName, folderName):
  mount = get_mount_point(mountName)
  source  = get_source(adlsContainerName, adlsAccountName, folderName)
  destroy_mount(mount)
  if not any(mount.mountPoint == mount for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(source = source, mount_point = mount, extra_configs = configs)

# COMMAND ----------

# Unmount only if directory is mounted
def destroy_mount(mountPoint):
  if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(mountPoint)


# COMMAND ----------

def importDatabase(dbmount, dbname):
  dirs = dbutils.fs.ls(dbmount)
  for dir in dirs:
    ddlSchema = "CREATE SCHEMA IF NOT EXISTS {}".format(dbname)
    ddl = "create table {schema}.{table} using DELTA location '{loc}'".format(schema = dbname, table = dir.name[:-1], loc = dir.path[5:-1])
    print(ddlSchema)
    spark.sql(ddlSchema)
    print(ddl)
    spark.sql(ddl)

# COMMAND ----------

CONFIG_STORAGE_NAME = 'dc50' + ENV + 'edpdlcfgsa'
create_mount('edpdlcfgsa', 'config', CONFIG_STORAGE_NAME, '')

# COMMAND ----------

L1_STORAGE_NAME = 'dc50' + ENV + 'edpdlsa0001'
create_mount('edpdlsa0001', 'edpl1', L1_STORAGE_NAME, '')

# COMMAND ----------

L02_STORAGE_NAME = 'dc50' + ENV + 'edpdlsa0002'
create_mount('edpdlsa0002', 'edpl2', L02_STORAGE_NAME, '')

# COMMAND ----------

importDatabase('/mnt/edpdlcfgsa/edp_metadata', 'edp_metadata')

# COMMAND ----------

importDatabase('/mnt/edpdlsa0001/edm', 'edpdlsa0001_edpl1_edm')

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create schema so the JETLI framework can add table
# MAGIC CREATE SCHEMA IF NOT EXISTS edpdlsa0002_edpl2_fdmgmtvds
