# Databricks notebook source
# MAGIC %run ./Setup-Common

# COMMAND ----------

# The user's name (email address) will be used to create a home directory into which all datasets will
# be written into so as to further isolating changes from other students running the same material.
raw_data_dir = f"{working_dir}/raw"

def path_exists(path):
  try:
    return BI.len(dbutils.fs.ls(path)) >= 0
  except Exception:
    return False
  

def install_datasets(reinstall):
  source_dir = f"wasbs://courseware@dbacademy.blob.core.windows.net/{dataset_name}/v01"
  
  BI.print(f"\nThe source directory for this dataset is\n{source_dir}/\n")
  existing = path_exists(raw_data_dir)

  if not reinstall and existing:
    BI.print(f"Skipping install of existing dataset to\n{raw_data_dir}")
    return 
  
  # Remove old versions of the previously installed datasets
  if existing:
    BI.print(f"Removing previously installed datasets from\n{raw_data_dir}/")
    dbutils.fs.rm(raw_data_dir, True)

  BI.print(f"""\nInstalling the datasets to {raw_data_dir}""")
  
  BI.print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the
      region that your workspace is in, this operation can take as little as 30 seconds and 
      upwards to 5 minutes, but this is a one-time operation.""")

  dbutils.fs.cp(source_dir, raw_data_dir, True)
  BI.print(f"""\nThe install of the datasets completed successfully.""")  

def reality_check_install():
  load_meta()
  
  suite_name = "install"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.cluster", validate_cluster_label, testFunction = validate_cluster, dependsOn=[suite.lastTestId()])
  
  suite.test(f"{suite_name}.reg_id", f"Valid Registration ID", testFunction = lambda: validate_registration_id(registration_id), dependsOn=[suite.lastTestId()])
  reg_id_id = suite.lastTestId()
  
  test_1_count = BI.len(dbutils.fs.ls(raw_data_dir+"/"))
  suite.testEquals(f"{suite_name}.root", f"Expected 3 files, found {test_1_count} in /", 3, test_1_count, dependsOn=reg_id_id)
  test_1_id = suite.lastTestId()
  
  test_2_count = BI.len(dbutils.fs.ls(raw_data_dir+"/_meta"))
  suite.testEquals(f"{suite_name}.meta", f"Expected 1 files, found {test_2_count} in /_meta", 1, test_2_count, dependsOn=reg_id_id)
  test_2_id = suite.lastTestId()
  
  test_3_count = BI.len(dbutils.fs.ls(raw_data_dir+"/products"))
  suite.testEquals(f"{suite_name}.products", f"Expected 2 files, found {test_3_count} in /products", 2, test_3_count, dependsOn=reg_id_id)
  test_3_id = suite.lastTestId()
  
  test_4_count = BI.len(dbutils.fs.ls(raw_data_dir+"/orders"))
  suite.testEquals(f"{suite_name}.orders", f"Expected 2 files, found {test_4_count} in /orders", 2, test_4_count, dependsOn=reg_id_id)
  test_4_id = suite.lastTestId()
  
  test_5_count = BI.len(dbutils.fs.ls(raw_data_dir+"/orders/batch"))
  suite.testEquals(f"{suite_name}.orders-batch", f"Expected 3 files, found {test_5_count} in /orders/batch", 3, test_5_count, dependsOn=reg_id_id)
  test_5_id = suite.lastTestId()
  
  test_6_count = BI.len(dbutils.fs.ls(raw_data_dir+"/orders/stream"))
  stream_count = spark.read.option("inferSchema", True).json(f"{raw_data_dir}/_meta/meta.json").first()["streamCount"]
  suite.testEquals(f"{suite_name}.orders-stream", f"Expected {stream_count} files, found {test_6_count} in /orders/stream", stream_count, test_6_count, dependsOn=reg_id_id)
  test_6_id = suite.lastTestId()

  previous_ids = [test_1_id, test_2_id, test_3_id, test_4_id, test_5_id, test_6_id]
  suite.test(f"{suite_name}.all", "All datasets were installed succesfully!", dependsOn=previous_ids,
             testFunction = lambda: True)
  
  daLogger.logSuite(suite_name, registration_id, suite)
  
  daLogger.logAggregatedResults(getLessonName(), registration_id, TestResultsAggregator)

  suite.displayResults()  
  
None # Suppress output

# COMMAND ----------

html = html_intro()
html += html_header()

html += html_row_fun("install_datasets()", "A utility function for installing datasets into the current workspace.")
html += html_row_fun("reality_check_install()", "A utility function for validating the install process.")

html += "</table></body></html>"

displayHTML(html)

