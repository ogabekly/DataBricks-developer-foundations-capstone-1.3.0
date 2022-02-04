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
html += html_products_table()
html += html_row_var("products_xml_path", products_xml_path, "The location of the product's XML file")

html += html_reality_check("reality_check_04_a()", "4.A")
html += html_reality_check("reality_check_04_b()", "4.B")
html += html_reality_check("reality_check_04_c()", "4.C")
html += html_reality_check_final("reality_check_04_final()")

html += "</table></body></html>"

displayHTML(html)

# COMMAND ----------

def createExpectedProductSchema():
  from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType, IntegerType, TimestampType, DoubleType, DecimalType, BooleanType
  return StructType([StructField("product_id", StringType(), True),
                     StructField("color", StringType(), True),
                     StructField("model_name", StringType(), True),
                     StructField("model_number", StringType(), True),
                     StructField("base_price", DoubleType(), True),
                     StructField("color_adj", DoubleType(), True),
                     StructField("size_adj", DoubleType(), True),
                     StructField("price", DoubleType(), True),
                     StructField("size", StringType(), True),
                    ])
  
expectedProductSchema = createExpectedProductSchema()

def createExpectedProductLineItemSchema():
  from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType, IntegerType, TimestampType, DoubleType, DecimalType, BooleanType
  return StructType([StructField("product_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_quantity", IntegerType(), True),
    StructField("product_sold_price", DecimalType(10,2), True),
    StructField("color", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("model_number", StringType(), True),
    StructField("base_price", DecimalType(10,2), True),
    StructField("color_adj", DecimalType(10,1), True),
    StructField("size_adj", DecimalType(10,2), True),
    StructField("price", DecimalType(10,2), True),
    StructField("size", StringType(), True),
                    ])
  
expectedProductLineItemSchema = createExpectedProductLineItemSchema()

check_a_passed = False
check_b_passed = False
check_c_passed = False
check_final_passed = False

# COMMAND ----------

def reality_check_04_a():
  global check_a_passed
  
  suite_name = "ex.04.a"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.cluster", validate_cluster_label, testFunction = validate_cluster, dependsOn=[suite.lastTestId()])
  cluster_id = suite.lastTestId()
  
  suite.test(f"{suite_name}.reg_id", f"Valid Registration ID", testFunction = lambda: validate_registration_id(registration_id), dependsOn=cluster_id)
  reg_id_id = suite.lastTestId()

  suite.test(f"{suite_name}.current-db", f"The current database is {user_db}", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: spark.catalog.currentDatabase() == user_db)

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_a_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def xml_installed():
    try:
      return spark.read.format("xml").option("rootTag", "products").option("rowTag", "product").load(products_xml_path).count() == meta_products_count+1
    except:
      return False

def reality_check_04_b():
  global check_b_passed
  
  suite_name = "ex.04.b"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.spark-xml", f"Successfully installed the spark-xml library", testFunction = xml_installed)
  
  check_b_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_04_c():
  global check_c_passed

  suite_name = "ex.04.c"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.current-db", f"The current database is {user_db}", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: spark.catalog.currentDatabase() == user_db)

  suite.test(f"{suite_name}.table-exists", f"The table {products_table} exists", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda t: t.name==products_table, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-managed", f"The table {products_table} is a managed table", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.list(BI.filter(lambda t: t.name==products_table, spark.catalog.listTables(user_db)))[0].tableType == "MANAGED")
  
  hive_path = f"dbfs:/user/hive/warehouse/{user_db}.db/{products_table}"
  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId()],
           testFunction = lambda: BI.len(BI.list(BI.filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( hive_path)))) == 1)

  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId()],
             testFunction = lambda: checkSchema(spark.read.table(products_table).schema, expectedProductSchema, False, False))

  suite.test(f"{suite_name}.count", f"Expected {meta_products_count} records", dependsOn=[suite.lastTestId()],
            testFunction = lambda: spark.read.table(products_table).count() == meta_products_count)

  suite.test(f"{suite_name}.min-color_adj", f"Sample A of color_adj (valid values)", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(products_table).select(FT.min(FT.col("color_adj"))).first()[0] == 1.0)

  suite.test(f"{suite_name}.max-color_adj", f"Sample B of color_adj (valid values)", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(products_table).select(FT.max(FT.col("color_adj"))).first()[0] == 1.1)

  suite.test(f"{suite_name}.min-size_adj", f"Sample A of size_adj (valid values)", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(products_table).select(FT.min(FT.col("size_adj"))).first()[0] == 0.9)

  suite.test(f"{suite_name}.max-size_adj", f"Sample B of size_adj (valid values)", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(products_table).select(FT.max(FT.col("size_adj"))).first()[0] == 1.0)

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_c_passed = suite.passed
  suite.displayResults()
  

# COMMAND ----------

def reality_check_04_final():
  global check_final_passed

  suite_name = "ex.04.all"
  suite = TestSuite()
  
  suite.testEquals(f"{suite_name}.a-passed", "Reality Check 04.A passed", check_a_passed, True)
  suite.testEquals(f"{suite_name}.b-passed", "Reality Check 04.B passed", check_b_passed, True)
  suite.testEquals(f"{suite_name}.c-passed", "Reality Check 04.C passed", check_c_passed, True)

  check_final_passed = suite.passed

  daLogger.logSuite(suite_name, registration_id, suite)
  
  daLogger.logAggregatedResults(getLessonName(), registration_id, TestResultsAggregator)
  
  suite.displayResults()

