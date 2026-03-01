# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add  to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@intechesg.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())


# COMMAND ----------

dbutils.fs.ls("/mnt/bronze")

# COMMAND ----------

def mount_if_not_exists(container, mount_point):
    mounts = [m.mountPoint for m in dbutils.fs.mounts()]
    
    if mount_point in mounts:
        print(f"✅ {mount_point} already mounted")
    else:
        dbutils.fs.mount(
            source=f"abfss://{container}@intechesg.dfs.core.windows.net/",
            mount_point=mount_point,
            extra_configs=configs
        )
        print(f"🚀 Mounted {mount_point}")


# COMMAND ----------

mount_if_not_exists("silver", "/mnt/silver")
mount_if_not_exists("gold", "/mnt/gold")


# COMMAND ----------

# MAGIC %sh
# MAGIC databricks secrets list-scopes
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks secrets create-scope --scope adls-scope
# MAGIC    