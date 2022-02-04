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
html += html_orders_table()
html += html_products_table()
html += html_line_items_table()

html += html_row_var("stream_path", stream_path, """The path to the stream directory of JSON orders""")
html += html_row_var("orders_checkpoint_path", orders_checkpoint_path, """The location of the checkpoint for streamed orders""")
html += html_row_var("line_items_checkpoint_path", line_items_checkpoint_path, """The location of the checkpoint for streamed line-items""")

html += html_reality_check("reality_check_05_a()", "5.A")
html += html_reality_check("reality_check_05_b()", "5.B")
html += html_reality_check("reality_check_05_c()", "5.C")
html += html_reality_check_final("reality_check_05_final()")

html += "</table></body></html>"

displayHTML(html)

# COMMAND ----------

def wait_for_stream_start(name, max_count):
  import time

  timeout = 60
  while BI.len(BI.list(BI.filter(lambda query: query.name == name, spark.streams.active))) == 0:
    BI.print(f"""Waiting for the stream "{name}" to start...""")
    time.sleep(5) # Give it a couple of seconds
    timeout -= 5  # Decrement our timer
    if (timeout <= 0): raise Exception("Timed out waiting for the query to start")

  query = BI.list(BI.filter(lambda query: query.name == name, spark.streams.active))[0]
  BI.print(f"""The stream "{name}" has started.""")
      
  timeout = 60
  while BI.len(query.recentProgress) == 0:
    BI.print(f"""The stream hasn't processed any trigger yet...""")
    time.sleep(5) # Give it a couple of seconds
    timeout -= 5  # Decrement our timer
    if (timeout <= 0): raise Exception("Timed out waiting for the first trigger")
    
  if BI.len(query.recentProgress) == 1: BI.print(f"""The stream has processed {BI.len(query.recentProgress)} triggers so far.""")
  else: BI.print(f"""The stream has processed {BI.len(query.recentProgress)} triggers so far.""")

  timeout = 60*5
  while BI.len(query.recentProgress) < max_count:
    BI.print(f"Processing trigger {BI.len(query.recentProgress)+1} of {max_count}...")
    if query.recentProgress[-1]["numInputRows"] > 1:
      raise Exception(f"Expected 1 record per trigger, found {query.recentProgress[-1]['numInputRows']}, aborting all tests.")

    time.sleep(5) # Give it a couple of seconds
    timeout -= 5  # Decrement our timer
    if (timeout <= 0): raise Exception("Timed out waiting for all events to be processed")
  
  return query

def first_n_equal_one(name):
  query = BI.list(BI.filter(lambda query: query.name == name, spark.streams.active))[0]
  for i in range(0, meta_stream_count):
    if query.recentProgress[i]["numInputRows"] != 1:
      return False
  return True

check_a_passed = False
check_b_passed = False
check_c_passed = False
check_d_passed = False
check_final_passed = False

# COMMAND ----------

def reality_check_05_a():
  global check_a_passed
  
  suite_name = "ex.05.a"
  suite = TestSuite()

  suite.test(f"{suite_name}.cluster", validate_cluster_label, testFunction = validate_cluster, dependsOn=[suite.lastTestId()])
  cluster_id = suite.lastTestId()
  
  suite.test(f"{suite_name}.reg_id", f"Valid Registration ID", testFunction = lambda: validate_registration_id(registration_id), dependsOn=cluster_id)
  reg_id_id = suite.lastTestId()
  
  suite.test(f"{suite_name}.current-db", f"The current database is {user_db}",  dependsOn=reg_id_id,
             testFunction = lambda: spark.catalog.currentDatabase() == user_db)

  suite.test(f"{suite_name}.o-total", f"Expected {meta_orders_count:,d} orders", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(orders_table).count() == meta_orders_count)

  suite.test(f"{suite_name}.li-total", f"Expected {meta_line_items_count:,d} line-items", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(line_items_table).count() == meta_line_items_count)
  
  suite.test(f"{suite_name}.p-count", f"Expected {meta_products_count} products", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: spark.read.table(products_table).count() == meta_products_count)

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_a_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_05_b():
  global check_b_passed
  
  suite_name = "ex.05.b"
  suite = TestSuite()

  try:
    query = wait_for_stream_start(orders_table, meta_stream_count)
    BI.print("Processing results...")

    suite.test(f"{suite_name}.min-count", f"Expected at least {meta_stream_count} triggers",
               testFunction = lambda: BI.len(query.recentProgress) >= meta_stream_count)

    suite.test(f"{suite_name}.max-count", f"Expected less than 100 triggers", dependsOn=[suite.lastTestId()], 
               testFunction = lambda: BI.len(query.recentProgress) < 100)

    suite.test(f"{suite_name}.whatever", f"Expected the first {meta_stream_count} triggers to processes 1 record per trigger", 
               dependsOn=[suite.lastTestId()], 
               testFunction = lambda: first_n_equal_one(orders_table))

    suite.test(f"{suite_name}.exists", "Checkpoint directory exists", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.len(dbutils.fs.ls(f"{orders_checkpoint_path}/metadata")) > 0)

    suite.test(f"{suite_name}.order_total", f"Expected {meta_orders_count + meta_stream_count:,d} orders ({meta_stream_count} new)", 
               dependsOn=[suite.lastTestId()], 
               testFunction = lambda: spark.read.table(orders_table).count() == meta_orders_count + meta_stream_count)

  except Exception as e:
    query = None
    suite.failPreReq(f"{suite_name}.prereq", e, [suite.lastTestId()])
  
  daLogger.logSuite(suite_name, registration_id, suite)
  
  if query:
    BI.print("Stopping the stream...")
    query.stop()

  check_b_passed = suite.passed
  suite.displayResults()


# COMMAND ----------

def reality_check_05_c():
  global check_c_passed
  
  suite_name = "ex.05.c"
  suite = TestSuite()

  try:
    query = wait_for_stream_start(line_items_table, meta_stream_count)
    BI.print("Processing results...")

    suite.test(f"{suite_name}.min-count", f"Expected at least {meta_stream_count:,d} triggers", 
               testFunction = lambda: BI.len(query.recentProgress) >= meta_stream_count)

    suite.test(f"{suite_name}.max-count", f"Expected less than 100 triggers", dependsOn=[suite.lastTestId()], 
               testFunction = lambda: BI.len(query.recentProgress) < 100)

    suite.test(f"{suite_name}.whatever", f"Expected the first {meta_stream_count:,d} triggers to processes 1 record per trigger", 
               dependsOn=[suite.lastTestId()], 
               testFunction = lambda: first_n_equal_one(line_items_table))

    suite.test(f"{suite_name}.exists", "Checkpoint directory exists", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.len(dbutils.fs.ls(f"{line_items_checkpoint_path}/metadata")) > 0)

    try:
      new_count = spark.read.json(stream_path).select("orderId", FT.explode("products")).count()
      expected_li_count = meta_line_items_count + new_count
      suite.test(f"{suite_name}.li_total", f"Expected {expected_li_count:,d} records, ({new_count} new)", 
                 dependsOn=[suite.lastTestId()], 
                 testFunction = lambda: spark.read.table(line_items_table).count() == expected_li_count)
    except Exception as e:
      suite.failPreReq(f"{suite_name}.prereq-a", e, [suite.lastTestId()])

  except Exception as e:
    query = None
    suite.failPreReq(f"{suite_name}.prereq-b", e, [suite.lastTestId()])
  
  daLogger.logSuite(suite_name, registration_id, suite)
  
  if query:
    BI.print("Stopping the stream...")
    query.stop()

  check_c_passed = suite.passed
  suite.displayResults()


# COMMAND ----------

def reality_check_05_final():
  global check_final_passed
  
  suite_name = "ex.05.all"
  suite = TestSuite()
  
  suite.testEquals(f"{suite_name}.a-passed", "Reality Check 05.A passed", check_a_passed, True)
  id_a = suite.lastTestId()
  
  suite.testEquals(f"{suite_name}.b-passed", "Reality Check 05.B passed", check_b_passed, True)
  id_b = suite.lastTestId()
  
  suite.testEquals(f"{suite_name}.c-passed", "Reality Check 05.C passed", check_c_passed, True)
  id_c = suite.lastTestId()
  
  suite.test(f"{suite_name}.order_total", f"Expected {meta_orders_count + meta_stream_count:,d} orders ({meta_stream_count} new)", 
             dependsOn=[id_a, id_b, id_c], 
             testFunction = lambda: spark.read.table(orders_table).count() == meta_orders_count + meta_stream_count)

  try:
    new_count = spark.read.json(stream_path).select("orderId", FT.explode("products")).count()
    expected_li_count = meta_line_items_count + new_count
    suite.test(f"{suite_name}.li_total", f"Expected {expected_li_count:,d} records, ({new_count} new)",  
                     dependsOn = [suite.lastTestId()], 
                     testFunction = lambda: spark.read.table(line_items_table).count() == expected_li_count)
  except Exception as e:
    suite.failPreReq(f"{suite_name}.prereq", e, [suite.lastTestId()])

  suite.test(f"{suite_name}.p-count", f"Expected {meta_products_count} products", 
             dependsOn = [suite.lastTestId()], 
             testFunction = lambda: spark.read.table(products_table).count() == meta_products_count)

  suite.test(f"{suite_name}.non-null-submitted_at", f"Non-null (properly parsed) submitted_at", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(orders_table).filter(FT.col("submitted_at").isNull()).count() == 0)
  
  check_final_passed = suite.passed

  daLogger.logSuite(suite_name, registration_id, suite)
  
  daLogger.logAggregatedResults(getLessonName(), registration_id, TestResultsAggregator)
  
  suite.displayResults()

