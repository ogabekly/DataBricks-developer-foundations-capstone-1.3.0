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
html += html_sales_reps_table()

html += html_row_var("question_1_results_table", question_1_results_table, """The name of the temporary view for the results to question #1.""")
html += html_row_var("question_2_results_table", question_1_results_table, """The name of the temporary view for the results to question #2.""")
html += html_row_var("question_3_results_table", question_1_results_table, """The name of the temporary view for the results to question #3.""")

html += html_reality_check("reality_check_06_a()", "6.A")
html += html_reality_check("reality_check_06_b()", "6.B")
html += html_reality_check("reality_check_06_c()", "6.C")
html += html_reality_check("reality_check_06_d()", "6.D")
html += html_reality_check_final("reality_check_06_final()")

html += "</table></body></html>"

displayHTML(html)

check_a_passed = False
check_b_passed = False
check_c_passed = False
check_d_passed = False
check_final_passed = False

# COMMAND ----------

def reality_check_06_a():
  global check_a_passed
  
  suite_name = "ex.06.a"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.cluster", validate_cluster_label, testFunction = validate_cluster, dependsOn=[suite.lastTestId()])
  cluster_id = suite.lastTestId()
  
  suite.test(f"{suite_name}.reg_id", f"Valid Registration ID", testFunction = lambda: validate_registration_id(registration_id), dependsOn=cluster_id)
  reg_id_id = suite.lastTestId()

  suite.test(f"{suite_name}.current-db", f"The current database is {user_db}",  dependsOn=reg_id_id,
             testFunction = lambda: spark.catalog.currentDatabase() == user_db)

  expected_order_count = meta_orders_count + meta_stream_count
  suite.test(f"{suite_name}.order_total", f"Expected {expected_order_count:,d} orders ({meta_stream_count} new)", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: expected_order_count == spark.read.table(orders_table).count())

  new_count = spark.read.json(stream_path).select("orderId", FT.explode("products")).count()
  expected_li_count = meta_line_items_count + new_count
  suite.test(f"{suite_name}.li_total", f"Expected {expected_li_count:,d} records ({new_count} new)", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(line_items_table).count() == expected_li_count)
  
  suite.test(f"{suite_name}.p-count", f"Expected {meta_products_count} products", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: meta_products_count == spark.read.table(products_table).count())
  
  suite.test(f"{suite_name}.sr-count", f"Expected {meta_sales_reps_count} sales reps", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: meta_sales_reps_count == spark.read.table(sales_reps_table).count())

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_a_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_06_b():
  global check_b_passed
  
  suite_name = "ex.06.b"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.table-exists", f"""The table "{question_1_results_table}" exists""",  
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda t: t.name==question_1_results_table, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-temp-view", f"""The table "{question_1_results_table}" is a temp view""", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.list(BI.filter(lambda t: t.name==question_1_results_table, spark.catalog.listTables(user_db)))[0].tableType == "TEMPORARY")

  try:
    exp_q1 = spark.read.table(orders_table).groupBy("shipping_address_state").count().orderBy(FT.col("count").desc()).collect()
    act_q1 = spark.read.table(question_1_results_table).collect()

    suite.test(f"{suite_name}.q1-state", f"""Schema contains the column "shipping_address_state".""", 
               dependsOn=[suite.lastTestId()],
               testFunction = lambda: "shipping_address_state" in spark.read.table(question_1_results_table).columns)

    suite.test(f"{suite_name}.q1-count", f"""Schema contains the column "count".""", 
               dependsOn=[suite.lastTestId()],
               testFunction = lambda: "count" in spark.read.table(question_1_results_table).columns)

    suite.test(f"{suite_name}.q1-first-state", f"""Expected the first state to be {exp_q1[0]["shipping_address_state"]}""", 
               dependsOn=[suite.lastTestId()],
               testFunction = lambda: exp_q1[0]["shipping_address_state"] == act_q1[0]["shipping_address_state"])

    suite.test(f"{suite_name}.q1-first-count", f"""Expected the first count to be {exp_q1[0]["count"]:,d}""", 
               dependsOn=[suite.lastTestId()],
               testFunction = lambda: exp_q1[0]["count"] == act_q1[0]["count"])

    suite.test(f"{suite_name}.q1-last-state", f"""Expected the last state to be {exp_q1[-1]["shipping_address_state"]}""", 
               dependsOn=[suite.lastTestId()],
               testFunction = lambda: exp_q1[-1]["shipping_address_state"] == act_q1[-1]["shipping_address_state"])

    suite.test(f"{suite_name}.q1-last-count", f"""Expected the last count to be {exp_q1[-1]["count"]:,d}""", 
               dependsOn=[suite.lastTestId()],
               testFunction = lambda: exp_q1[-1]["count"] == act_q1[-1]["count"])

  except Exception as e:
    suite.fail(f"{suite_name}.exception", dependsOn=[suite.lastTestId()],
               description=f"""Able to execute prerequisite query.<p style='max-width: 1024px; overflow-x:auto'>{e}</p>""")
    
  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_b_passed = suite.passed
  suite.displayResults()


# COMMAND ----------

def reality_check_06_c(ex_avg, ex_min, ex_max):
  global check_c_passed
  
  suite_name = "ex.06.c"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.table-exists", f"""The table "{question_2_results_table}" exists""",  
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda t: t.name==question_2_results_table, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-temp-view", f"""The table "{question_2_results_table}" is a temp view""", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.list(BI.filter(lambda t: t.name==question_2_results_table, spark.catalog.listTables(user_db)))[0].tableType == "TEMPORARY")

  suite.test(f"{suite_name}.q2-avg", f"""Schema contains the column "avg(product_sold_price)".""", dependsOn=[suite.lastTestId()],
             testFunction = lambda: "avg(product_sold_price)" in spark.read.table(question_2_results_table).columns)
  
  suite.test(f"{suite_name}.q2-min", f"""Schema contains the column "min(product_sold_price)".""", dependsOn=[suite.lastTestId()],
             testFunction = lambda: "min(product_sold_price)" in spark.read.table(question_2_results_table).columns)
  
  suite.test(f"{suite_name}.q2-max", f"""Schema contains the column "max(product_sold_price)".""", dependsOn=[suite.lastTestId()],
             testFunction = lambda: "max(product_sold_price)" in spark.read.table(question_2_results_table).columns)
  
  try:
    act_results = spark.read.table(question_2_results_table).first()
    act_avg = act_results["avg(product_sold_price)"]
    act_min = act_results["min(product_sold_price)"]
    act_max = act_results["max(product_sold_price)"]

    exp_results = spark.read.table(orders_table).join(spark.read.table(sales_reps_table), "sales_rep_id").join(spark.read.table(line_items_table), "order_id").join(spark.read.table(products_table), "product_id").filter(FT.col("shipping_address_state") == "NC").filter(FT.col("_error_ssn_format") == True).filter(FT.col("color") == "green").select(FT.avg("product_sold_price"), FT.min("product_sold_price"), FT.max("product_sold_price")).first()
    exp_avg = exp_results["avg(product_sold_price)"]
    exp_min = exp_results["min(product_sold_price)"]
    exp_max = exp_results["max(product_sold_price)"]
    
    suite.testEquals(f"{suite_name}.q2-tv-avg", f"""Expected the temp view's average to be {exp_avg}""", exp_avg, act_avg, dependsOn=[suite.lastTestId()])
    suite.testEquals(f"{suite_name}.q2-tv-min", f"""Expected the temp view's minimum to be {exp_min}""", exp_min, act_min, dependsOn=[suite.lastTestId()])
    suite.testEquals(f"{suite_name}.q2-tv-max", f"""Expected the temp view's maximum to be {exp_max}""", exp_max, act_max, dependsOn=[suite.lastTestId()])

    suite.testEquals(f"{suite_name}.q2-ex-avg", f"""Expected the extracted average to be {exp_avg}""", exp_avg, ex_avg, dependsOn=[suite.lastTestId()])
    suite.testEquals(f"{suite_name}.q2-ex-min", f"""Expected the extracted minimum to be {exp_min}""", exp_min, ex_min, dependsOn=[suite.lastTestId()])
    suite.testEquals(f"{suite_name}.q2-ex-max", f"""Expected the extracted maximum to be {exp_max}""", exp_max, ex_max, dependsOn=[suite.lastTestId()])

  except Exception as e:
    suite.fail(f"{suite_name}.exception", dependsOn=[suite.lastTestId()],
               description=f"""Able to execute prerequisite query.<p style='max-width: 1024px; overflow-x:auto'>{e}</p>""")

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_c_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_06_d():
  global check_d_passed
  
  suite_name = "ex.06.d"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.table-exists", f"""The table "{question_3_results_table}" exists""",  
             testFunction = lambda: BI.len(BI.list(BI.filter(lambda t: t.name==question_3_results_table, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-temp-view", f"""The table "{question_3_results_table}" is a temp view""", dependsOn=[suite.lastTestId()],
             testFunction = lambda: BI.list(BI.filter(lambda t: t.name==question_3_results_table, spark.catalog.listTables(user_db)))[0].tableType == "TEMPORARY")

  suite.test(f"{suite_name}.q2-fn", f"""Schema contains the column "sales_rep_first_name".""", dependsOn=[suite.lastTestId()],
            testFunction = lambda: "sales_rep_first_name" in spark.read.table(question_3_results_table).columns)
  
  suite.test(f"{suite_name}.q2-ln", f"""Schema contains the column "sales_rep_last_name".""", dependsOn=[suite.lastTestId()],
            testFunction = lambda: "sales_rep_last_name" in spark.read.table(question_3_results_table).columns)

  suite.test(f"{suite_name}.count", f"""Expected 1 record""", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(question_3_results_table).count() == 1)

  try:
    act_results = spark.read.table(question_3_results_table).first()
    act_first = act_results["sales_rep_first_name"]
    act_last = act_results["sales_rep_last_name"]

    exp_results = (spark.read.table(orders_table).join(
                   spark.read.table(sales_reps_table), "sales_rep_id").join(
                   spark.read.table(line_items_table), "order_id").join(
                   spark.read.table(products_table), "product_id").withColumn("per_product_profit", FT.col("product_sold_price") - FT.col("price")).withColumn("total_profit", FT.col("per_product_profit") * FT.col("product_quantity")).groupBy("sales_rep_id", "sales_rep_first_name", "sales_rep_last_name").sum("total_profit").orderBy(FT.col("sum(total_profit)").desc()).first())
    exp_first = exp_results["sales_rep_first_name"]
    exp_last = exp_results["sales_rep_last_name"]

    suite.testEquals(f"{suite_name}.q3-fn", f"""Expected the first name to be {exp_first}""", exp_first, act_first, dependsOn=[suite.lastTestId()])
    suite.testEquals(f"{suite_name}.q3-ln", f"""Expected the last name to be {exp_last}""", exp_last, act_last, dependsOn=[suite.lastTestId()])

  except Exception as e:
    suite.fail(f"{suite_name}.exception", dependsOn=[suite.lastTestId()],
               description=f"""Able to execute prerequisite query.<p style='max-width: 1024px; overflow-x:auto'>{e}</p>""")

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_d_passed = suite.passed
  suite.displayResults()
  

# COMMAND ----------

def reality_check_06_final():
  global check_final_passed
  
  suite_name = "ex.06.all"
  suite = TestSuite()
  
  suite.testEquals(f"{suite_name}.a-passed", "Reality Check 06.A passed", check_a_passed, True)
  suite.testEquals(f"{suite_name}.b-passed", "Reality Check 06.B passed", check_b_passed, True)
  suite.testEquals(f"{suite_name}.c-passed", "Reality Check 06.C passed", check_c_passed, True)
  suite.testEquals(f"{suite_name}.d-passed", "Reality Check 06.D passed", check_d_passed, True)

  check_final_passed = suite.passed
  
  daLogger.logSuite(suite_name, registration_id, suite)
  
  daLogger.logAggregatedResults(getLessonName(), registration_id, TestResultsAggregator)
  
  suite.displayResults()

