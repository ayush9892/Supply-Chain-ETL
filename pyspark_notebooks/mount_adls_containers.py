# Databricks notebook source
def mount_adls(storage_account, container_name):
    # Get id and secrets
    client_id = dbutils.secrets.get('secrets-scope', 'supplychain-proj-client-id')
    tenant_id = dbutils.secrets.get('secrets-scope', 'supplychain-proj-tenant-id')
    client_secret = dbutils.secrets.get('secrets-scope', 'supplychain-proj-client-secret')

    # Set spark configs
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        }
    
    # Unmount mount if its already exists
    if any(mount.mountPoint == f'/mnt/{storage_account}/{container_name}' for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f'/mnt/{storage_account}/{container_name}')

    # Mount the container
    dbutils.fs.mount(
        source= f'abfss://{container_name}@{storage_account}.dfs.core.windows.net/',
        mount_point= f'/mnt/{storage_account}/{container_name}',
        extra_configs= configs
    )
    
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Raw Container

# COMMAND ----------

mount_adls('supplychainprojadls', 'raw')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Bronze Container

# COMMAND ----------

mount_adls('supplychainprojadls', 'bronze')

# COMMAND ----------

mount_adls('supplychainprojadls', 'silver')

# COMMAND ----------

mount_adls('supplychainprojadls', 'gold')

# COMMAND ----------


