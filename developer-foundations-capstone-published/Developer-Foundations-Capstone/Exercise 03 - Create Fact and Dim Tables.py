# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise #3 - Create Fact & Dim Tables
# MAGIC 
# MAGIC Now that the three years of orders are combined into a single dataset, we can begin the processes of transforming the data.
# MAGIC 
# MAGIC In the one record, there are actually four sub-datasets:
# MAGIC * The order itself which is the aggregator of the other three datasets.
# MAGIC * The line items of each order which includes the price and quantity of each specific item.
# MAGIC * The sales rep placing the order.
# MAGIC * The customer placing the order - for the sake of simplicity, we will **not** break this dataset out and leave it as part of the order.
# MAGIC 
# MAGIC What we want to do next, is to extract all that data into their respective datasets (except the customer data). 
# MAGIC 
# MAGIC In other words, we want to normalize the data, in this case, to reduce data duplication.
# MAGIC 
# MAGIC This exercise is broken up into 5 steps:
# MAGIC * Exercise 3.A - Create & Use Database
# MAGIC * Exercise 3.B - Load & Cache Batch Orders
# MAGIC * Exercise 3.C - Extract Sales Reps
# MAGIC * Exercise 3.D - Extract Orders
# MAGIC * Exercise 3.E - Extract Line Items

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Setup Exercise #3</h2>
# MAGIC 
# MAGIC To get started, we first need to configure your Registration ID and then run the setup notebook.

# COMMAND ----------

# MAGIC %md ### Setup - Registration ID
# MAGIC 
# MAGIC In the next command, please update the variable **`registration_id`** with the Registration ID you received when you signed up for this project.
# MAGIC 
# MAGIC For more information, see [Registration ID]($./Registration ID)

# COMMAND ----------

registration_id = "FILL_IN"

# COMMAND ----------

# MAGIC %md ### Setup - Run the exercise setup
# MAGIC 
# MAGIC Run the following cell to setup this exercise, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-03

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3.A - Create &amp; Use Database</h2>
# MAGIC 
# MAGIC By using a specific database, we can avoid contention to commonly named tables that may be in use by other users of the workspace.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Create the database identified by the variable **`user_db`**
# MAGIC * Use the database identified by the variable **`user_db`** so that any tables created in this notebook are **NOT** added to the **`default`** database
# MAGIC 
# MAGIC **Special Notes**
# MAGIC * Do not hard-code the database name - in some scenarios this will result in validation errors.
# MAGIC * For assistence with the SQL command to create a database, see <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-database.html" target="_blank">CREATE DATABASE</a> on the Databricks docs website.
# MAGIC * For assistence with the SQL command to use a database, see <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-usedb.html" target="_blank">USE DATABASE</a> on the Databricks docs website.

# COMMAND ----------

# MAGIC %md ### Implement Exercise #3.A
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #3.A
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_03_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3.B - Load &amp; Cache Batch Orders</h2>
# MAGIC 
# MAGIC Next, we need to load the batch orders from the previous exercise and then cache them in preparation to transform the data later in this exercise.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Load the delta dataset we created in the previous exercise, identified by the variable **`batch_source_path`**.
# MAGIC * Using that same dataset, create a temporary view identified by the variable **`batch_temp_view`**.
# MAGIC * Cache the temporary view.

# COMMAND ----------

# MAGIC %md ### Implement Exercise #3.B
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #3.B
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_03_b()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3.C - Extract Sales Reps</h2>
# MAGIC 
# MAGIC Our batched orders from Exercise #2 contains thousands of orders and with every order, is the name, SSN, address and other information on the sales rep making the order.
# MAGIC 
# MAGIC We can use this data to create a table of just our sales reps.
# MAGIC 
# MAGIC If you consider that we have only ~100 sales reps, but thousands of orders, we are going to have a lot of duplicate data in this space.
# MAGIC 
# MAGIC Also unique to this set of data, is the fact that social security numbers were not always sanitized meaning sometime they were formatted with hyphens and in other cases they were not - this is something we will have to address here.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Load the table **`batched_orders`** (identified by the variable **`batch_temp_view`**)
# MAGIC * The SSN numbers have errors in them that we want to track - add the **`boolean`** column **`_error_ssn_format`** - for any case where **`sales_rep_ssn`** has a hypen in it, set this value to **`true`** otherwise **`false`**
# MAGIC * Convert various columns from their string representation to the specified type:
# MAGIC   * The column **`sales_rep_ssn`** should be represented as a **`Long`** (Note: You will have to first clean the column by removing extreneous hyphens in some records)
# MAGIC   * The column **`sales_rep_zip`** should be represented as an **`Integer`**
# MAGIC * Remove the columns not directly related to the sales-rep record:
# MAGIC   * Unrelated ID columns: **`submitted_at`**, **`order_id`**, **`customer_id`**
# MAGIC   * Shipping address columns: **`shipping_address_attention`**, **`shipping_address_address`**, **`shipping_address_city`**, **`shipping_address_state`**, **`shipping_address_zip`**
# MAGIC   * Product columns: **`product_id`**, **`product_quantity`**, **`product_sold_price`**
# MAGIC * Because there is one record per product ordered (many products per order), not to mention one sales rep placing many orders (many orders per sales rep), there will be duplicate records for our sales reps. Remove all duplicate records, making sure to exclude **`ingest_file_name`** and **`ingested_at`** from the evaluation of duplicate records
# MAGIC * Load the dataset to the managed delta table **`sales_rep_scd`** (identified by the variable **`sales_reps_table`**)
# MAGIC 
# MAGIC **Additional Requirements:**<br/>
# MAGIC The schema for the **`sales_rep_scd`** table must be:
# MAGIC * **`sales_rep_id`**:**`string`**
# MAGIC * **`sales_rep_ssn`**:**`long`**
# MAGIC * **`sales_rep_first_name`**:**`string`**
# MAGIC * **`sales_rep_last_name`**:**`string`**
# MAGIC * **`sales_rep_address`**:**`string`**
# MAGIC * **`sales_rep_city`**:**`string`**
# MAGIC * **`sales_rep_state`**:**`string`**
# MAGIC * **`sales_rep_zip`**:**`integer`**
# MAGIC * **`ingest_file_name`**:**`string`**
# MAGIC * **`ingested_at`**:**`timestamp`**
# MAGIC * **`_error_ssn_format`**:**`boolean`**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #3.C
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #3.C
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_03_c()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3.D - Extract Orders</h2>
# MAGIC 
# MAGIC Our batched orders from Exercise 02 contains one line per product meaning there are multiple records per order.
# MAGIC 
# MAGIC The goal of this step is to extract just the order details (excluding the sales rep and line items)
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Load the table **`batched_orders`** (identified by the variable **`batch_temp_view`**)
# MAGIC * Convert various columns from their string representation to the specified type:
# MAGIC   * The column **`submitted_at`** is a "unix epoch" (number of seconds since 1970-01-01 00:00:00 UTC) and should be represented as a **`Timestamp`**
# MAGIC   * The column **`shipping_address_zip`** should be represented as an **`Integer`**
# MAGIC * Remove the columns not directly related to the order record:
# MAGIC   * Sales reps columns: **`sales_rep_ssn`**, **`sales_rep_first_name`**, **`sales_rep_last_name`**, **`sales_rep_address`**, **`sales_rep_city`**, **`sales_rep_state`**, **`sales_rep_zip`**
# MAGIC   * Product columns: **`product_id`**, **`product_quantity`**, **`product_sold_price`**
# MAGIC * Because there is one record per product ordered (many products per order), there will be duplicate records for each order. Remove all duplicate records, making sure to exclude **`ingest_file_name`** and **`ingested_at`** from the evaluation of duplicate records
# MAGIC * Add the column **`submitted_yyyy_mm`** which is a **`string`** derived from **`submitted_at`** and is formatted as "**yyyy-MM**".
# MAGIC * Load the dataset to the managed delta table **`orders`** (identified by the variable **`orders_table`**)
# MAGIC   * In thise case, the data must also be partitioned by **`submitted_yyyy_mm`**
# MAGIC 
# MAGIC **Additional Requirements:**
# MAGIC * The schema for the **`orders`** table must be:
# MAGIC   * **`submitted_at:timestamp`**
# MAGIC   * **`submitted_yyyy_mm`** using the format "**yyyy-MM**"
# MAGIC   * **`order_id:string`**
# MAGIC   * **`customer_id:string`**
# MAGIC   * **`sales_rep_id:string`**
# MAGIC   * **`shipping_address_attention:string`**
# MAGIC   * **`shipping_address_address:string`**
# MAGIC   * **`shipping_address_city:string`**
# MAGIC   * **`shipping_address_state:string`**
# MAGIC   * **`shipping_address_zip:integer`**
# MAGIC   * **`ingest_file_name:string`**
# MAGIC   * **`ingested_at:timestamp`**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #3.D
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #3.D
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_03_d()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3.E - Extract Line Items</h2>
# MAGIC 
# MAGIC Now that we have extracted sales reps and orders, we next want to extract the specific line items of each order.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Load the table **`batched_orders`** (identified by the variable **`batch_temp_view`**)
# MAGIC * Retain the following columns (see schema below)
# MAGIC   * The correlating ID columns: **`order_id`** and **`product_id`**
# MAGIC   * The two product-specific columns: **`product_quantity`** and **`product_sold_price`**
# MAGIC   * The two ingest columns: **`ingest_file_name`** and **`ingested_at`**
# MAGIC * Convert various columns from their string representation to the specified type:
# MAGIC   * The column **`product_quantity`** should be represented as an **`Integer`**
# MAGIC   * The column **`product_sold_price`** should be represented as an **`Decimal`** with two decimal places as in **`decimal(10,2)`**
# MAGIC * Load the dataset to the managed delta table **`line_items`** (identified by the variable **`line_items_table`**)
# MAGIC 
# MAGIC **Additional Requirements:**
# MAGIC * The schema for the **`line_items`** table must be:
# MAGIC   * **`order_id`**:**`string`**
# MAGIC   * **`product_id`**:**`string`**
# MAGIC   * **`product_quantity`**:**`integer`**
# MAGIC   * **`product_sold_price`**:**`decimal(10,2)`**
# MAGIC   * **`ingest_file_name`**:**`string`**
# MAGIC   * **`ingested_at`**:**`timestamp`**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #3.E
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #3.E
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_03_e()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3 - Final Check</h2>
# MAGIC 
# MAGIC Run the following command to make sure this exercise is complete:

# COMMAND ----------

reality_check_03_final()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
