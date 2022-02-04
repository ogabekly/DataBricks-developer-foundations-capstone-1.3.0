# Databricks notebook source
# MAGIC %run ./Setup-Common

# COMMAND ----------

load_meta()

# COMMAND ----------

html = html_intro()
html += html_header()

html += html_username()
html += html_working_dir()

html += html_user_db()
html += html_batch_source_path()

html += html_orders_table()
html += html_line_items_table()
html += html_sales_reps_table()
html += html_row_var("batch_temp_view", batch_temp_view, """The name of the temp view used in this exercise""")

html += html_reality_check("reality_check_03_a()", "3.A")
html += html_reality_check("reality_check_03_b()", "3.B")
html += html_reality_check("reality_check_03_c()", "3.C")
html += html_reality_check("reality_check_03_d()", "3.D")
html += html_reality_check("reality_check_03_e()", "3.E")
html += html_reality_check_final("reality_check_03_final()")

html += "</table></body></html>"

displayHTML(html)

# COMMAND ----------

def createExpectedSalesRepSchema():
  from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType, IntegerType, TimestampType, DoubleType, BooleanType
  return StructType([StructField("sales_rep_id", StringType(), True),
                     StructField("sales_rep_ssn", LongType(), True),
                     StructField("sales_rep_first_name", StringType(), True),
                     StructField("sales_rep_last_name", StringType(), True),
                     StructField("sales_rep_address", StringType(), True),
                     StructField("sales_rep_city", StringType(), True),
                     StructField("sales_rep_state", StringType(), True),
                     StructField("sales_rep_zip", IntegerType(), True),
                     StructField("ingest_file_name", StringType(), True),
                     StructField("ingested_at", TimestampType(), True),
                     StructField("_error_ssn_format", BooleanType(), True),
                    ])
expectedSalesRepSchema = createExpectedSalesRepSchema()

def createExpectedOrdersSchema():
  from pyspark.sql.types import StructType, StructField, ArrayType, DecimalType, StringType, DateType, LongType, IntegerType, TimestampType, DoubleType, BooleanType
  return StructType([StructField("submitted_at", TimestampType(), True),
                     StructField("submitted_yyyy_mm", StringType(), True),
                     StructField("order_id", StringType(), True),
                     StructField("customer_id", StringType(), True),
                     StructField("sales_rep_id", StringType(), True),
                     
                     StructField("shipping_address_attention", StringType(), True),
                     StructField("shipping_address_address", StringType(), True),
                     StructField("shipping_address_city", StringType(), True),
                     StructField("shipping_address_state", StringType(), True),
                     StructField("shipping_address_zip", IntegerType(), True),

                     StructField("ingest_file_name", StringType(), True),
                     StructField("ingested_at", TimestampType(), True),
                    ])
expectedOrdersSchema = createExpectedOrdersSchema()

def createLineItemSchema():
  from pyspark.sql.types import StructType, StructField, ArrayType, DecimalType, StringType, DateType, LongType, IntegerType, TimestampType, DoubleType, BooleanType
  return StructType([StructField("order_id", StringType(), True),
                     StructField("product_id", StringType(), True),
                     
                     StructField("product_quantity", IntegerType(), True),
                     StructField("product_sold_price", DecimalType(10,2), True),

                     StructField("ingest_file_name", StringType(), True),
                     StructField("ingested_at", TimestampType(), True),
                    ])
expectedLineItemsSchema = createLineItemSchema()

check_a_passed = False
check_b_passed = False
check_c_passed = False
check_d_passed = False
check_e_passed = False
check_final_passed = False

# COMMAND ----------

def reality_check_03_a():
  global check_a_passed
  
  suite_name = "ex.03.a"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.cluster", validate_cluster_label, testFunction = validate_cluster, dependsOn=[suite.lastTestId()])
  cluster_id = suite.lastTestId()
  
  suite.test(f"{suite_name}.reg_id", f"Valid Registration ID", testFunction = lambda: validate_registration_id(registration_id), dependsOn=cluster_id)

  suite.test(f"{suite_name}.current-db", f"The current database is {user_db}", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.catalog.currentDatabase() == user_db)

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_a_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_03_b():
  global check_b_passed

  suite_name = "ex.03.b"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.current-db", f"The current database is {user_db}", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: spark.catalog.currentDatabase() == user_db)

  suite.test(f"{suite_name}.table-exists", f"The table {batch_temp_view} exists", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda t: t.name==batch_temp_view, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-temp-view", f"The table {batch_temp_view} is a temp view", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.list(BI.filter(lambda t: t.name==batch_temp_view, spark.catalog.listTables(user_db)))[0].tableType == "TEMPORARY")
  
  suite.test(f"{suite_name}.is-cached", f"The table {batch_temp_view} is cached", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.catalog.isCached(batch_temp_view))

  suite.test(f"{suite_name}.count", f"Expected {meta_batch_count_2017+meta_batch_count_2018+meta_batch_count_2019:,d} records", dependsOn=[suite.lastTestId()],
             testFunction = lambda:  spark.read.table(batch_temp_view).count() == meta_batch_count_2017+meta_batch_count_2018+meta_batch_count_2019)
  
  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_b_passed = suite.passed
  suite.displayResults()


# COMMAND ----------

def reality_check_03_c():
  global check_c_passed

  hive_path = f"dbfs:/user/hive/warehouse/{user_db}.db/{sales_reps_table}"

  suite_name = "ex.03.c"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.current-db", f"The current database is {user_db}", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: spark.catalog.currentDatabase() == user_db)
  
  suite.test(f"{suite_name}.table-exists", f"The table {sales_reps_table} exists", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda t: t.name==sales_reps_table, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-managed", f"The table {sales_reps_table} is a managed table", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: BI.list(BI.filter(lambda t: t.name==sales_reps_table, spark.catalog.listTables(user_db)))[0].tableType == "MANAGED")

  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( hive_path)))) == 1)

  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: checkSchema(spark.read.table(sales_reps_table).schema, expectedSalesRepSchema, False, False))

  suite.test(f"{suite_name}.count-total", f"Expected {meta_sales_reps_count:,d} records", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(sales_reps_table).count() == meta_sales_reps_count)

  suite.test(f"{suite_name}.count-ssn-format", f"Expected _error_ssn_format record count to be {meta_ssn_format_count:,d}", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: spark.read.table(sales_reps_table).filter("_error_ssn_format == true").count() == meta_ssn_format_count)

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_c_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_03_d():
  global check_d_passed

  suite_name = "ex.03.d"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.current-db", f"The current database is {user_db}", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: spark.catalog.currentDatabase() == user_db)
  
  suite.test(f"{suite_name}.table-exists", f"The table {orders_table} exists", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda t: t.name==orders_table, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-managed", f"The table {orders_table} is a managed table", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.list(BI.filter(lambda t: t.name==orders_table, spark.catalog.listTables(user_db)))[0].tableType == "MANAGED")

  hive_path = f"dbfs:/user/hive/warehouse/{user_db}.db/{orders_table}"
  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId()],
           testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( hive_path)))) == 1)
  
  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId()],
             testFunction = lambda: checkSchema(spark.read.table(orders_table).schema, expectedOrdersSchema, False, False))

  suite.test(f"{suite_name}.count-total", f"Expected {meta_orders_count:,d} records", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(orders_table).count() == meta_orders_count)

  suite.test(f"{suite_name}.non-null-submitted_at", f"Non-null (properly parsed) submitted_at", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(orders_table).filter(FT.col("submitted_at").isNull()).count() == 0)

  def is_partitioned():
    files = BI.filter(lambda p: p.endswith("_delta_log/") == False, BI.map(lambda f: f.path, dbutils.fs.ls(hive_path)))
    
    for y in range(2017, 2020):
      for m in ["01","02","03","04","05","06","07","08","09","10","11","12"]:
        path = f"{hive_path}/submitted_yyyy_mm={y}-{m}"
        if path in files == False:
          return False
    return True
  
  suite.test(f"{suite_name}.is_partitioned", f"Partitioned by submitted_yyyy_mm", dependsOn=[suite.lastTestId()], 
             testFunction = is_partitioned)

  suite.test(f"{suite_name}.partitions", f"Found 36 partitions", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda p: p.endswith("_delta_log/") == False, BI.map(lambda f: f.path, dbutils.fs.ls(hive_path))))) == 36)
  
  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_d_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_03_e():
  global check_e_passed

  suite_name = "ex.03.e"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.current-db", f"The current database is {user_db}", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: spark.catalog.currentDatabase() == user_db)

  suite.test(f"{suite_name}.table-exists", f"The table {line_items_table} exists", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda t: t.name==line_items_table, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-managed", f"The table {line_items_table} is a managed table", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.list(BI.filter(lambda t: t.name==line_items_table, spark.catalog.listTables(user_db)))[0].tableType == "MANAGED")

  hive_path = f"dbfs:/user/hive/warehouse/{user_db}.db/{line_items_table}"
  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId()],
           testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( hive_path)))) == 1)
  
  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId()],
             testFunction = lambda: checkSchema(spark.read.table(line_items_table).schema, expectedLineItemsSchema, False, False))

  suite.test(f"{suite_name}.count-total", f"Expected {meta_line_items_count:,d} records", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(line_items_table).count() == meta_line_items_count)

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_e_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_03_final():
  global check_final_passed

  suite_name = "ex.03.all"
  suite = TestSuite()
  
  suite.testEquals(f"{suite_name}.a-passed", "Reality Check 03.A passed", check_a_passed, True)
  suite.testEquals(f"{suite_name}.b-passed", "Reality Check 03.B passed", check_b_passed, True)
  suite.testEquals(f"{suite_name}.c-passed", "Reality Check 03.C passed", check_c_passed, True)
  suite.testEquals(f"{suite_name}.d-passed", "Reality Check 03.D passed", check_d_passed, True)
  suite.testEquals(f"{suite_name}.e-passed", "Reality Check 03.E passed", check_e_passed, True)
    
  check_final_passed = suite.passed
  
  daLogger.logSuite(suite_name, registration_id, suite)
  
  daLogger.logAggregatedResults(getLessonName(), registration_id, TestResultsAggregator)
  
  suite.displayResults()

