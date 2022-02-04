# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise #5 - Streaming Orders
# MAGIC 
# MAGIC With our four historical datasets properly loaded, we can now begin to process the "current" orders.
# MAGIC 
# MAGIC In this case, the new "system" is landing one JSON file per order into cloud storage.
# MAGIC 
# MAGIC We can process these JSON files as a stream of orders under the assumption that new orders are continually added to this dataset.
# MAGIC 
# MAGIC In order to keep this project simple, we have reduced the "stream" of orders to just the first few hours of 2020 and will be throttling that stream to only one file per iteration.
# MAGIC 
# MAGIC This exercise is broken up into 3 steps:
# MAGIC * Exercise 5.A - Use Database
# MAGIC * Exercise 5.B - Stream-Append Orders
# MAGIC * Exercise 5.C - Stream-Append Line Items
# MAGIC 
# MAGIC ## Some Friendly Advice...
# MAGIC 
# MAGIC Each record is a JSON object with roughly the following structure:
# MAGIC 
# MAGIC * **`customerID`**
# MAGIC * **`orderId`**
# MAGIC * **`products`**
# MAGIC   * array
# MAGIC     * **`productId`**
# MAGIC     * **`quantity`**
# MAGIC     * **`soldPrice`**
# MAGIC * **`salesRepId`**
# MAGIC * **`shippingAddress`**
# MAGIC   * **`address`**
# MAGIC   * **`attention`**
# MAGIC   * **`city`**
# MAGIC   * **`state`**
# MAGIC   * **`zip`**
# MAGIC * **`submittedAt`**
# MAGIC 
# MAGIC As you ingest this data, it will need to be transformed to match the existing **`orders`** table's schema and the **`line_items`** table's schema.
# MAGIC 
# MAGIC Before attempting to ingest the data as a stream, we highly recomend that you start with a static **`DataFrame`** so that you can iron out the various kinks:
# MAGIC * Renaming and flattening columns
# MAGIC * Exploding the products array
# MAGIC * Parsing the **`submittedAt`** column into a **`timestamp`**
# MAGIC * Conforming to the **`orders`** and **`line_items`** schemas - because these are Delta tables, appending to them will fail if the schemas are not correct
# MAGIC 
# MAGIC Furthermore, creating a stream from JSON files will first require you to specify the schema - you can "cheat" and infer that schema from some of the JSON files before starting the stream.

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Setup Exercise #5</h2>
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

# MAGIC %run ./_includes/Setup-Exercise-05

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #5.A - Use Database</h2>
# MAGIC 
# MAGIC Each notebook uses a different Spark session and will initially use the **`default`** database.
# MAGIC 
# MAGIC As in the previous exercise, we can avoid contention to commonly named tables by using our user-specific database.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Use the database identified by the variable **`user_db`** so that any tables created in this notebook are **NOT** added to the **`default`** database

# COMMAND ----------

# MAGIC %md ### Implement Exercise #5.A
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #5.A
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_05_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #5.B - Stream-Append Orders</h2>
# MAGIC 
# MAGIC Every JSON file ingested by our stream representes one order and the enumerated list of products purchased in that order.
# MAGIC 
# MAGIC Our goal is simple, ingest the data, transform it as required by the **`orders`** table's schema, and append these new records to our existing table.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 
# MAGIC * Ingest the stream of JSON files:
# MAGIC   * Start a stream from the path identified by **`stream_path`**.
# MAGIC   * Using the **`maxFilesPerTrigger`** option, throttle the stream to process only one file per iteration.
# MAGIC   * Add the ingest meta data (same as with our other datasets):
# MAGIC     * **`ingested_at`**:**`timestamp`**
# MAGIC     * **`ingest_file_name`**:**`string`**
# MAGIC   * Properly parse the **`submitted_at`**  as a valid **`timestamp`**
# MAGIC   * Add the column **`submitted_yyyy_mm`** usinge the format "**yyyy-MM**"
# MAGIC   * Make any other changes required to the column names and data types so that they conform to the **`orders`** table's schema
# MAGIC 
# MAGIC * Write the stream to a Delta **table**.:
# MAGIC   * The table's format should be "**delta**"
# MAGIC   * Partition the data by the column **`submitted_yyyy_mm`**
# MAGIC   * Records must be appended to the table identified by the variable **`orders_table`**
# MAGIC   * The query must be named the same as the table, identified by the variable **`orders_table`**
# MAGIC   * The query must use the checkpoint location identified by the variable **`orders_checkpoint_path`**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #5.B
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #5.B
# MAGIC Run the following command to ensure that you are on track.
# MAGIC 
# MAGIC **Caution**: In the cell above, you will be appending to a Delta table and the final record count will be validated below. Should you restart the stream, you will inevitably append duplicate records to these tables forcing the validation to fail. There are two things you will need to address in this scenario:
# MAGIC * Address the duplicate data issue by re-running **Exercise #3** which would presumably delete and/or overwrite the datasets, putting them back to their default state for this exercise.
# MAGIC * Address the stream's state issue (remembering which files were processed) by deleting the directory identified by *`orders_checkpoint_path`*

# COMMAND ----------

reality_check_05_b()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #5.C - Stream-Append Line Items</h2>
# MAGIC 
# MAGIC The same JSON file we processed in the previous stream also contains the line items which we now need to extract and append to the existing **`line_items`** table.
# MAGIC 
# MAGIC Just like before, our goal is simple, ingest the data, transform it as required by the **`line_items`** table's schema, and append these new records to our existing table.
# MAGIC 
# MAGIC Note: we are processing the same stream twice - there are other patterns to do this more efficiently, but for this exercise, we want to keep the design simple.<br/>
# MAGIC The good news here is that you can copy most of the code from the previous step to get you started here.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 
# MAGIC * Ingest the stream of JSON files:
# MAGIC   * Start a stream from the path identified by **`stream_path`**.
# MAGIC   * Using the **`maxFilesPerTrigger`** option, throttle the stream to process only one file per iteration.
# MAGIC   * Add the ingest meta data (same as with our other datasets):
# MAGIC     * **`ingested_at`**:**`timestamp`**
# MAGIC     * **`ingest_file_name`**:**`string`**
# MAGIC   * Make any other changes required to the column names and data types so that they conform to the **`line_items`** table's schema
# MAGIC     * The most significant transformation will be to the **`products`** column.
# MAGIC     * The **`products`** column is an array of elements and needs to be exploded (see **`pyspark.sql.functions`**)
# MAGIC     * One solution would include:
# MAGIC       1. Select **`order_id`** and explode **`products`** while renaming it to **`product`**.
# MAGIC       2. Flatten the **`product`** column's nested values.
# MAGIC       3. Add the ingest meta data (**`ingest_file_name`** and **`ingested_at`**).
# MAGIC       4. Convert data types as required by the **`line_items`** table's schema.
# MAGIC 
# MAGIC * Write the stream to a Delta sink:
# MAGIC   * The sink's format should be "**delta**"
# MAGIC   * Records must be appended to the table identified by the variable **`line_items_table`**
# MAGIC   * The query must be named the same as the table, identified by the variable **`line_items_table`**
# MAGIC   * The query must use the checkpoint location identified by the variable **`line_items_checkpoint_path`**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #5.C
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

# COMMAND ----------

reality_check_05_c()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #5 - Final Check</h2>
# MAGIC 
# MAGIC Run the following command to make sure this exercise is complete:

# COMMAND ----------

reality_check_05_final()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
