# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./Setup-Common

# COMMAND ----------

# Excludes lesson name so as to drop all lessons.
base_dir = f"dbfs:/user/{clean_username}/dbacademy/{clean_course_name}"
print(f"""Deleting the course directory...""")

dbutils.fs.rm(base_dir, True)
print(f"""Deleted "{base_dir}" """)

# COMMAND ----------

# Excludes lesson name so as to drop all databases.
db_name = f"""dbacademy_{clean_username}_{clean_course_name}"""
print(f"""Dropping all databases that start with "{db_name}" """)

for name in list(filter(lambda name: name.startswith(db_name), map(lambda d: d.name, spark.catalog.listDatabases()))):
  spark.sql(f"DROP DATABASE IF EXISTS {name} CASCADE")
  print(f"""Dropped the database "{name}".""")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
