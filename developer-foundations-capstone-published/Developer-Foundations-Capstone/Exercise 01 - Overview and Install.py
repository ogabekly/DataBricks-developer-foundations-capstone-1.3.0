# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # Exercise #1 - Project Overview & Dataset Install
# MAGIC 
# MAGIC The capstone project aims to assess rudimentary skills as it relates to the Apache Spark and DataFrame APIs.
# MAGIC 
# MAGIC The approach taken here assumes that you are familiar with and have some experience with the following entities:
# MAGIC * **`SparkContext`**
# MAGIC * **`SparkSession`**
# MAGIC * **`DataFrame`**
# MAGIC * **`DataFrameReader`**
# MAGIC * **`DataFrameWriter`**
# MAGIC * The various functions found in the module **`pyspark.sql.functions`**
# MAGIC 
# MAGIC Throughout this project, you will be given specific instructions and it is our expectation that you will be able to complete these instructions drawing on your existing knowledge as well as other sources such as the <a href="https://spark.apache.org/docs/latest/api.html" target="_blank">Spark API Documentation</a>.
# MAGIC 
# MAGIC After reviewing the project, the datasets and the various exercises, we will install the<br/>
# MAGIC datasets into your Databricks workspace so that you may proceed with this capstone project.

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Project Overview</h2>
# MAGIC * The Project - an introduction to this project
# MAGIC * The Data - an introduction to this project's datasets
# MAGIC * The Exercises - an overview of the various exercises in this project

# COMMAND ----------

# MAGIC %md ### The Project
# MAGIC 
# MAGIC The idea behind this project is to ingest data from a purchasing system and load it into a data lake for further analysis. 
# MAGIC 
# MAGIC Each exercise is broken up into smaller steps, or milestones.
# MAGIC 
# MAGIC After every milestone, we have provided a "reality check" to help ensure that you are progressing as expected.
# MAGIC 
# MAGIC Please note, because each exercise builds on the previous, it is essential to complete all exercises and ensure their "reality checks" pass, before moving on to the next exercise.
# MAGIC 
# MAGIC As the last exercise of this project, we will use this data we loaded to answer some simple business questions.

# COMMAND ----------

# MAGIC %md ### The Data
# MAGIC The raw data comes in three forms:
# MAGIC 
# MAGIC 1. Orders that were processed in 2017, 2018 and 2019.
# MAGIC   * For each year a separate batch (or backup) of that year's orders was produced
# MAGIC   * The format of all three files are similar, but were not produced exactly the same:
# MAGIC     * 2017 is in a fixed-width text file format
# MAGIC     * 2018 is tab-separated text file
# MAGIC     * 2019 is comma-separated text file
# MAGIC   * Each order consists for four main data points:
# MAGIC     0. The order - the highest level aggregate
# MAGIC     0. The line items - the individual products purchased in the order
# MAGIC     0. The sales reps - the person placing the order
# MAGIC     0. The customer - the person who purchased the items and where it was shipped.
# MAGIC   * All three batches are consistent in that there is one record per line item creating a significant amount of duplicated data across orders, reps and customers.
# MAGIC   * All entities are generally referenced by an ID, such as order_id, customer_id, etc.
# MAGIC   
# MAGIC 2. All products to be sold by this company (SKUs) are represented in a single XML file
# MAGIC 
# MAGIC 3. In 2020, the company switched systems and now lands a single JSON file in cloud storage for every order received.
# MAGIC   * These orders are simplified versions of the batched data fro 2017-2019 and includes only the order's details, the line items, and the correlating ids
# MAGIC   * The sales reps's data is no longer represented in conjunction with an order

# COMMAND ----------

# MAGIC %md ### The Exercises
# MAGIC 
# MAGIC * In **Exercise #1**, (this notebook) we introduce the registration procedure, the installation of our datasets and the reality-checks meant to aid you in your progress thought this capstone project.
# MAGIC 
# MAGIC * In **Exercise #2**, we will ingest the batch data for 2017-2019, combine them into a single dataset for future processing.
# MAGIC 
# MAGIC * In **Exercise #3**, we will take the unified batch data from **Exercise #2**, clean it, and extract it into three new datasets: Orders, Line Items and Sales Reps. The customer data, for the sake of simplicity, will not be broken out and left with the orders.
# MAGIC 
# MAGIC * In **Exercise #4**, we will ingest the XML document containing all the projects, and combine it with the Line Items to create yet another dataset, Product Line Items.
# MAGIC 
# MAGIC * In **Exercise #5**, we will begin processing the stream of orders for 2020, appending that stream of data to the existing datasets as necessary.
# MAGIC 
# MAGIC * In **Exercise #6**, we will use all of our new datasets to answer a handful of business questions.
# MAGIC 
# MAGIC * In **Exercise #7**, we provide final instructions for submitting your capstone project.

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #1 - Install Datasets</h2>
# MAGIC 
# MAGIC The datasets for this project are stored in a public object store.
# MAGIC 
# MAGIC They need to be downloaded and installed into your Databricks workspace before proceeding with this project.
# MAGIC 
# MAGIC But before doing that, we need to configure a cluster appropriate for this project.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 1. Configure the cluster (see specific instructions below)
# MAGIC 2. Attach this notebook to your cluster
# MAGIC 3. Specify your Registration ID
# MAGIC 4. Run the setup notebook for this exercise
# MAGIC 5. Install the datasets
# MAGIC 6. Run the reality check to verify the datasets were correctly installed
# MAGIC 
# MAGIC Note: These steps represent the basic pattern used by each exercise in this capstone project

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Setup Exercise #1</h2>
# MAGIC 
# MAGIC To get started, we first need to configure your Registration ID and then run the setup notebook.

# COMMAND ----------

# MAGIC %md ### Setup - Create A Cluster
# MAGIC 
# MAGIC #### Databricks Community Edition
# MAGIC 
# MAGIC This Capstone project was designed to work with Databricks Runtime Version (DBR) 9.1 LTS and the Databricks Community Edition's (CE) default cluster configuration. 
# MAGIC 
# MAGIC When working in CE, start a default cluster, specify **DBR 9.1 LTS**, and then proceede with the next step. 
# MAGIC 
# MAGIC #### Other than Community Edition (MSA, AWS or GCP)
# MAGIC 
# MAGIC This capstone project was designed to work with a small, single-node cluster when not using CE. When configuring your cluster, please specify the following:
# MAGIC 
# MAGIC * DBR: **9.1 LTS** 
# MAGIC * Cluster Mode: **Single Node**
# MAGIC * Node Type: 
# MAGIC   * for Microsoft Azure - **Standard_E4ds_v4**
# MAGIC   * for Amazon Web Services - **i3.xlarge** 
# MAGIC   * for Google Cloud Platform - **n1-highmem-4** 
# MAGIC 
# MAGIC Please feel free to use the Community Edition if the recomended node types are not available.

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

# MAGIC %run ./_includes/Setup-Exercise-01

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #1 - Install Datasets</h2>
# MAGIC 
# MAGIC Simply run the following command to install the capstone's datasets into your workspace.

# COMMAND ----------

# At any time during this project, you can reinstall the source datasets
# by setting reinstall=True. These datasets will not be automtically 
# reinstalled when this notebook is re-ran so as to save you time.
install_datasets(reinstall=False)

# COMMAND ----------

# MAGIC %md ### Reality Check #1
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_install()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
