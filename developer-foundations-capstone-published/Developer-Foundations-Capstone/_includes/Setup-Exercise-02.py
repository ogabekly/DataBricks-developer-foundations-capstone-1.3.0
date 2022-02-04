# Databricks notebook source
# MAGIC %run ./Setup-Common

# COMMAND ----------

load_meta()

# COMMAND ----------

html = html_intro()
html += html_header()

html += html_username()
html += html_working_dir()

html += html_row_var("batch_2017_path", batch_2017_path, """The path to the 2017 batch of orders""")
html += html_row_var("batch_2018_path", batch_2018_path, """The path to the 2018 batch of orders""")
html += html_row_var("batch_2019_path", batch_2019_path, """The path to the 2019 batch of orders""")
html += html_row_var("batch_target_path", batch_target_path, """The location of the new, unified, raw, batch of orders & sales reps""")


html += html_reality_check("reality_check_02_a()", "2.A")
html += html_reality_check("reality_check_02_b()", "2.B")
html += html_reality_check("reality_check_02_c()", "2.C")
html += html_reality_check_final("reality_check_02_final()")

html += "</table></body></html>"

displayHTML(html)

# COMMAND ----------

def createExpectedSchema():
  from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
  return StructType([StructField("submitted_at", StringType(), True),
                     StructField("order_id", StringType(), True),
                     StructField("customer_id", StringType(), True),
                     StructField("sales_rep_id", StringType(), True),
                     StructField("sales_rep_ssn", StringType(), True),
                     StructField("sales_rep_first_name", StringType(), True),
                     StructField("sales_rep_last_name", StringType(), True),
                     StructField("sales_rep_address", StringType(), True),
                     StructField("sales_rep_city", StringType(), True),
                     StructField("sales_rep_state", StringType(), True),
                     StructField("sales_rep_zip", StringType(), True),
                     StructField("shipping_address_attention", StringType(), True),
                     StructField("shipping_address_address", StringType(), True),
                     StructField("shipping_address_city", StringType(), True),
                     StructField("shipping_address_state", StringType(), True),
                     StructField("shipping_address_zip", StringType(), True),
                     StructField("product_id", StringType(), True),
                     StructField("product_quantity", StringType(), True),
                     StructField("product_sold_price", StringType(), True),
                     StructField("ingest_file_name", StringType(), True),
                     StructField("ingested_at", TimestampType(), True)])

def no_white_space():
  for column in string_columns:
    if 0 != spark.read.format("delta").load(batch_target_path).filter(FT.col(column) != FT.trim(FT.col(column))).count():
      return False
  return True

def no_empty_strings():
  for column in string_columns:
    if 0 != spark.read.format("delta").load(batch_target_path).filter(FT.trim(FT.col(column)) == FT.lit("")).count():
      return False
  return True

def no_null_strings():
  for column in string_columns:
    if 0 != spark.read.format("delta").load(batch_target_path).filter(FT.trim(FT.col(column)) == FT.lit("null")).count():
      return False
  return True

def valid_ingest_file_name_2017():
  return valid_ingest_file_name(2017, "txt")

def valid_ingest_file_name_2018():
  return valid_ingest_file_name(2018, "csv")

def valid_ingest_file_name_2019():
  return valid_ingest_file_name(2019, "csv")

def valid_ingest_file_name(expected_year, expected_ext):
  try:
    tempDF = spark.read.format("delta").load(batch_target_path).withColumn("submitted_at", FT.from_unixtime("submitted_at").cast("timestamp"))
    if 0 != tempDF.filter(FT.year(FT.col("submitted_at")) == expected_year).filter(FT.col("ingest_file_name").endswith(f"{username}/dbacademy/{dataset_name}/raw/orders/batch/{expected_year}.{expected_ext}") == False).count():
      return False
    return True
  except Exception as e:
    BI.print(e)
    return False

def valid_ingest_date_2017():
  valid_ingest_date(meta_batch_count_2017)
  
def valid_ingest_date_2018():
  valid_ingest_date(meta_batch_count_2017+meta_batch_count_2018)
  
def valid_ingest_date_2019():
  valid_ingest_date(meta_batch_count_2017+meta_batch_count_2018+meta_batch_count_2019)
  
def valid_ingest_date(expected):
  from datetime import datetime
  
  today = datetime.today()
  actual = spark.read.format("delta").load(batch_target_path).filter(FT.year("ingested_at") == today.year).filter(FT.month("ingested_at") == today.month).filter(FT.dayofmonth("ingested_at") == today.day).count()
  return actual == expected

def valid_values():
  if 0 != spark.read.format("delta").load(batch_target_path).select(FT.length(FT.col("submitted_at")).alias("length")).filter("length != 10").count(): return False
  if 0 != spark.read.format("delta").load(batch_target_path).select(FT.length(FT.col("order_id")).alias("length")).filter("length != 36").count(): return False
  if 0 != spark.read.format("delta").load(batch_target_path).select(FT.length(FT.col("customer_id")).alias("length")).filter("length != 36").count(): return False
  if 0 != spark.read.format("delta").load(batch_target_path).select(FT.length(FT.col("sales_rep_id")).alias("length")).filter("length != 36").count(): return False
  if 0 != spark.read.format("delta").load(batch_target_path).select(FT.length(FT.col("sales_rep_ssn")).alias("length")).filter(FT.col("length").isin(11,9) == False).count(): return False
  if 0 != spark.read.format("delta").load(batch_target_path).select(FT.length(FT.col("sales_rep_state")).alias("length")).filter("length != 2").count(): return False
  if 0 != spark.read.format("delta").load(batch_target_path).select(FT.length(FT.col("sales_rep_zip")).alias("length")).filter("length != 5").count(): return False
  if 0 != spark.read.format("delta").load(batch_target_path).select(FT.length(FT.col("shipping_address_zip")).alias("length")).filter("length != 5").count(): return False
  if 0 != spark.read.format("delta").load(batch_target_path).select(FT.length(FT.col("product_id")).alias("length")).filter("length != 36").count(): return False
  return True

expectedSchema = createExpectedSchema()

expected_columns = BI.list(BI.map(lambda f: f.name, expectedSchema))

string_columns = expected_columns.copy()
string_columns.remove("ingested_at")

check_a_passed = False
check_b_passed = False
check_c_passed = False
check_final_passed = False

# COMMAND ----------

def reality_check_02_a():
  global check_a_passed
  
  suite_name = "ex.02.a"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.cluster", validate_cluster_label, testFunction = validate_cluster, dependsOn=[suite.lastTestId()])
  cluster_id = suite.lastTestId()
  
  suite.test(f"{suite_name}.reg_id", f"Valid Registration ID", testFunction = lambda: validate_registration_id(registration_id), dependsOn=cluster_id)
  reg_id_id = suite.lastTestId()

  suite.test(f"{suite_name}.exists", "Target directory exists", dependsOn=reg_id_id, 
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith("/"), dbutils.fs.ls( batch_target_path)))) > 0)
  
  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( batch_target_path)))) == 1)
  
  suite.test(f"{suite_name}.has_parquet", "Found at least one Parquet part-file", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith(".parquet"), dbutils.fs.ls( batch_target_path)))) > 0)
  
  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: checkSchema(spark.read.format("delta").load(batch_target_path).schema, expectedSchema, False, False))

  suite.test(f"{suite_name}.count", f"Expected {meta_batch_count_2017:,d} records", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.format("delta").load(batch_target_path).count() == meta_batch_count_2017)

  suite.test(f"{suite_name}.white-space", "No leading or trailing whitespace in column values, need to trim", testFunction=no_white_space, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.empty-strings", "No empty strings in column values, should be the SQL value null", testFunction=no_empty_strings, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.null-strings", "No \"null\" strings for column values, should be the SQL value null", testFunction=no_null_strings, dependsOn=[suite.lastTestId()])
  
  suite.test(f"{suite_name}.ingest-file", "Ingest file names are valid for 2017", testFunction=valid_ingest_file_name_2017, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.ingest-date", "Ingest date is valid for 2017", testFunction=valid_ingest_date_2017, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.values", "Key columns are the correct length (properly parsed)", testFunction=valid_values, dependsOn=[suite.lastTestId()])
  
  daLogger.logSuite(suite_name, registration_id, suite)

  check_a_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_02_b():
  global check_b_passed

  suite_name = "ex.02.b"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.exists", "Target directory exists",  
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith("/"), dbutils.fs.ls( batch_target_path)))) > 0)

  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( batch_target_path)))) == 1)
  
  suite.test(f"{suite_name}.has_parquet", "Found at least one Parquet part-file", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith(".parquet"), dbutils.fs.ls( batch_target_path)))) > 0)
  
  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: checkSchema(spark.read.format("delta").load(batch_target_path).schema, expectedSchema, False, False))
  
  suite.test(f"{suite_name}.count", f"Expected {meta_batch_count_2017+meta_batch_count_2018:,d} records", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: spark.read.format("delta").load(batch_target_path).count() == meta_batch_count_2017+meta_batch_count_2018)
  
  suite.test(f"{suite_name}.white-space", "No leading or trailing whitespace in column values, need to trim", testFunction=no_white_space, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.empty-strings", "No empty strings in column values, should be the SQL value null", testFunction=no_empty_strings, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.null-strings", "No \"null\" strings for column values, should be the SQL value null", testFunction=no_null_strings, dependsOn=[suite.lastTestId()])
  
  suite.test(f"{suite_name}.ingest-file", "Ingest file names are valid for 2018", testFunction=valid_ingest_file_name_2018, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.ingest-date", "Ingest date is valid for 2018", testFunction=valid_ingest_date_2018, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.values", "Key columns are the correct length (properly parsed)", testFunction=valid_values, dependsOn=[suite.lastTestId()])

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_b_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_02_c():
  global check_c_passed

  suite_name = "ex.02.c"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.exists", "Target directory exists",  
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith("/"), dbutils.fs.ls( batch_target_path)))) > 0)
  
  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( batch_target_path)))) == 1)
  
  suite.test(f"{suite_name}.has_parquet", "Found at least one Parquet part-file", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith(".parquet"), dbutils.fs.ls( batch_target_path)))) > 0)
  
  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: checkSchema(spark.read.format("delta").load(batch_target_path).schema, expectedSchema, False, False))

  suite.test(f"{suite_name}.count", f"Expected {meta_batch_count_2017+meta_batch_count_2018+meta_batch_count_2019:,d} records", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: spark.read.format("delta").load(batch_target_path).count() == meta_batch_count_2017+meta_batch_count_2018+meta_batch_count_2019)

  suite.test(f"{suite_name}.white-space", "No leading or trailing whitespace in column values, need to trim", testFunction=no_white_space, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.empty-strings", "No empty strings in column values, should be the SQL value null", testFunction=no_empty_strings, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.null-strings", "No \"null\" strings for column values, should be the SQL value null", testFunction=no_null_strings, dependsOn=[suite.lastTestId()])
  
  suite.test(f"{suite_name}.ingest-file", "Ingest file names are valid for 2019", testFunction=valid_ingest_file_name_2019, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.ingest-date", "Ingest date is valid for 2019", testFunction=valid_ingest_date_2019, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.values", "Key columns are the correct length (properly parsed)", testFunction=valid_values, dependsOn=[suite.lastTestId()])

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_c_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_02_final():
  global check_final_passed

  suite_name = "ex.02.all"
  suite = TestSuite()
  
  suite.testEquals(f"{suite_name}.a-passed", "Reality Check 02.A passed", check_a_passed, True)
  id_a = suite.lastTestId()
  
  suite.testEquals(f"{suite_name}.b-passed", "Reality Check 02.B passed", check_b_passed, True)
  id_b = suite.lastTestId()
  
  suite.testEquals(f"{suite_name}.c-passed", "Reality Check 02.C passed", check_c_passed, True)
  id_c = suite.lastTestId()
  
  suite.test(f"{suite_name}.exists", "Target directory exists", dependsOn=[id_a, id_b, id_c],  
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith("/"), dbutils.fs.ls( batch_target_path)))) > 0)
  
  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( batch_target_path)))) == 1)
  
  suite.test(f"{suite_name}.has_parquet", "Found at least one Parquet part-file", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith(".parquet"), dbutils.fs.ls( batch_target_path)))) > 0)

  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: checkSchema(spark.read.format("delta").load(batch_target_path).schema, expectedSchema, False, False))

  suite.test(f"{suite_name}.count", f"Expected {meta_batch_count_2017+meta_batch_count_2018+meta_batch_count_2019:,d} records", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: spark.read.format("delta").load(batch_target_path).count() == meta_batch_count_2017+meta_batch_count_2018+meta_batch_count_2019)

  suite.test(f"{suite_name}.white-space", "No leading or trailing whitespace in column values, need to trim", testFunction=no_white_space, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.empty-strings", "No empty strings in column values, should be the SQL value null", testFunction=no_empty_strings, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.null-strings", "No \"null\" strings for column values, should be the SQL value null", testFunction=no_null_strings, dependsOn=[suite.lastTestId()])

  suite.test(f"{suite_name}.ingest-file-2017", "Ingest file names are valid for 2017", testFunction=valid_ingest_file_name_2017, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.ingest-date-2017", "Ingest date is valid for 2017", testFunction=valid_ingest_date_2017, dependsOn=[suite.lastTestId()])

  suite.test(f"{suite_name}.ingest-file-2018", "Ingest file names are valid for 2018", testFunction=valid_ingest_file_name_2018, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.ingest-date-2018", "Ingest date is valid for 2018", testFunction=valid_ingest_date_2018, dependsOn=[suite.lastTestId()])

  suite.test(f"{suite_name}.ingest-file-2019", "Ingest file names are valid for 2019", testFunction=valid_ingest_file_name_2019, dependsOn=[suite.lastTestId()])
  suite.test(f"{suite_name}.ingest-date-2019", "Ingest date is valid for 2019", testFunction=valid_ingest_date_2019, dependsOn=[suite.lastTestId()])
  
  suite.test(f"{suite_name}.values", "Key columns are the correct length (properly parsed)", testFunction=valid_values, dependsOn=[suite.lastTestId()])
    
  check_final_passed = suite.passed
    
  daLogger.logSuite(suite_name, registration_id, suite)
  
  daLogger.logAggregatedResults(getLessonName(), registration_id, TestResultsAggregator)
 
  suite.displayResults()

