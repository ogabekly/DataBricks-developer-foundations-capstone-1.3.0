# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise #4 - XML Ingestion, Products Table
# MAGIC 
# MAGIC The products being sold by our sales reps are itemized in an XML document which we will need to load.
# MAGIC 
# MAGIC Unlike CSV, JSON, Parquet, & Delta, support for XML is not included with the default distribution of Apache Spark.
# MAGIC 
# MAGIC Before we can load the XML document, we need additional support for a **`DataFrameReader`** that can processes XML files.
# MAGIC 
# MAGIC Once the **spark-xml** library is installed to our cluster, we can load our XML document and proceede with our other transformations.
# MAGIC 
# MAGIC This exercise is broken up into 4 steps:
# MAGIC * Exercise 4.A - Use Database
# MAGIC * Exercise 4.B - Install Library
# MAGIC * Exercise 4.C - Load Products
# MAGIC * Exercise 4.D - Load ProductLineItems

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Setup Exercise #4</h2>
# MAGIC 
# MAGIC To get started, we first need to configure your Registration ID and then run the setup notebook.

# COMMAND ----------

# MAGIC %md ### Setup - Registration ID
# MAGIC 
# MAGIC In the next commmand, please update the variable **`registration_id`** with the Registration ID you received when you signed up for this project.
# MAGIC 
# MAGIC For more information, see [Registration ID]($./Registration ID)

# COMMAND ----------

registration_id = "FILL_IN"

# COMMAND ----------

# MAGIC %md ### Setup - Run the exercise setup
# MAGIC 
# MAGIC Run the following cell to setup this exercise, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-04

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #4.A - Use Database</h2>
# MAGIC 
# MAGIC Each notebook uses a different Spark session and will initially use the **`default`** database.
# MAGIC 
# MAGIC As in the previous exercise, we can avoid contention to commonly named tables by using our user-specific database.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Use the database identified by the variable **`user_db`** so that any tables created in this notebook are **NOT** added to the **`default`** database

# COMMAND ----------

# MAGIC %md ### Implement Exercise #4.A
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #4.A
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_04_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #4.B - Install Library</h2>
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Register the **spark-xml** library - edit your cluster configuration and then from the **Libraries** tab, install the following library:
# MAGIC   * Type: **Maven**
# MAGIC   * Coordinates: **com.databricks:spark-xml_2.12:0.10.0**
# MAGIC 
# MAGIC If you are unfamiliar with this processes, more information can be found in the <a href="https://docs.databricks.com/libraries/cluster-libraries.html" target="_blank">Cluster libraries documentation</a>.
# MAGIC 
# MAGIC Once the library is installed, run the following reality check to confirm proper installation.<br/>
# MAGIC Note: You may need to restart the cluster after installing the library for you changes to take effect.

# COMMAND ----------

reality_check_04_b()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #4.C - Load Products</h2>
# MAGIC 
# MAGIC With the **spark-xml** library installed, ingesting an XML document is identical to ingesting any other dataset - other than specific, provided, options.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Load the XML document using the following paramters:
# MAGIC   * Format: **xml**
# MAGIC   * Options:
# MAGIC     * **`rootTag`** = **`products`** - identifies the root tag in the XML document, in our case this is "products"
# MAGIC     * **`rowTag`** = **`product`** - identifies the tag of each row under the root tag, in our case this is "product"
# MAGIC     * **`inferSchema`** = **`True`** - The file is small, and a one-shot operation - infering the schema will save us some time
# MAGIC   * File Path: specified by the variable **`products_xml_path`**
# MAGIC   
# MAGIC * Update the schema to conform to the following specification:
# MAGIC   * **`product_id`**:**`string`**
# MAGIC   * **`color`**:**`string`**
# MAGIC   * **`model_name`**:**`string`**
# MAGIC   * **`model_number`**:**`string`**
# MAGIC   * **`base_price`**:**`double`**
# MAGIC   * **`color_adj`**:**`double`**
# MAGIC   * **`size_adj`**:**`double`**
# MAGIC   * **`price`**:**`double`**
# MAGIC   * **`size`**:**`string`**
# MAGIC 
# MAGIC * Exclude any records for which a **`price`** was not included - these represent products that are not yet available for sale.
# MAGIC * Load the dataset to the managed delta table **`products`** (identified by the variable **`products_table`**)

# COMMAND ----------

# MAGIC %md ### Implement Exercise #4.C
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #4.C
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_04_c()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #4 - Final Check</h2>
# MAGIC 
# MAGIC Run the following command to make sure this exercise is complete:

# COMMAND ----------

reality_check_04_final()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
