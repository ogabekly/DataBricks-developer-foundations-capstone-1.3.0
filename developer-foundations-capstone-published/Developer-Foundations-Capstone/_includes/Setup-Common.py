# Databricks notebook source
import builtins as BI
from pyspark.sql import functions as FT

# Get all tags
def getTags() -> dict: 
  return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
    dbutils.entry_point.getDbutils().notebook().getContext().tags()
  )

# Get a single tag's value
def getTag(tagName: str, defaultValue: str = None) -> str:
  values = getTags()[tagName]
  try:
    if BI.len(values) > 0:
      return values
  except:
    return defaultValue

def getLessonName() -> str:
  return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")[-1]

padding = "{padding}"
border_color = "#CDCDCD"
font = "font-size:16px;"
weight = "font-weight:bold;"
align = "vertical-align:top;"
border = f"border-bottom:1px solid {border_color};"

def html_intro():
  return """<html><body><table style="width:100%">
            <p style="font-size:16px">The following variables and functions have been defined for you.<br/>Please refer to them in the following instructions.</p>"""

def html_row_var(name, value, description):
  return f"""<tr><td style="{font} {weight} {padding} {align} white-space:nowrap; color:green;">{name}</td>
                 <td style="{font} {weight} {padding} {align} white-space:nowrap; color:blue;">{value}</td>
             </tr><tr><td style="{border} {font} {padding}">&nbsp;</td>
                 <td style="{border} {font} {padding} {align}">{description}</td></tr>"""

def html_row_fun(name, description):
  return f"""<tr><td style="{border}; {font} {padding} {align} {weight} white-space:nowrap; color:green;">{name}</td>
                 <td style="{border}; {font} {padding} {align}">{description}</td></td></tr>"""

def html_header():
  return f"""<tr><th style="{border} padding: 0 1em 0 0; text-align:left">Variable/Function</th>
                 <th style="{border} padding: 0 1em 0 0; text-align:left">Description</th></tr>"""

def html_username():
  return html_row_var("username", username, """This is the email address that you signed into Databricks with""")

def html_working_dir():
  return html_row_var("working_dir", working_dir, """This is the directory in which all work should be conducted""")

def html_user_db():
  return html_row_var("user_db", user_db, """The name of the database you will use for this project.""")

def html_orders_table():
  return html_row_var("orders_table", orders_table, """The name of the orders table.""")

def html_sales_reps_table():
  return html_row_var("sales_reps_table", sales_reps_table, """The name of the sales reps table.""")

def html_products_table():
  return html_row_var("products_table", products_table, """The name of the products table.""")

def html_line_items_table():
  return html_row_var("line_items_table", line_items_table, """The name of the line items table.""")

batch_path_desc = """The location of the combined, raw, batch of orders."""
def html_batch_source_path():
  return html_row_var("batch_source_path", batch_target_path, batch_path_desc)
def html_batch_target_path():
  return html_row_var("batch_target_path", batch_target_path, batch_path_desc)

def html_reality_check_final(fun_name):
  return html_row_fun(fun_name, """A utility function for validating the entire exercise""")
  
def html_reality_check(fun_name, exercise):
  return html_row_fun(fun_name, f"""A utility function for validating Exercise #{exercise}""")

def checkSchema(schemaA, schemaB, keepOrder=True, keepNullable=False): 
  # Usage: checkSchema(schemaA, schemaB, keepOrder=false, keepNullable=false)
  
  from pyspark.sql.types import StructField
  
  if (schemaA == None and schemaB == None):
    return True
  
  elif (schemaA == None or schemaB == None):
    return False
  
  else:
    schA = schemaA
    schB = schemaB

    if (keepNullable == False):  
        schA = [StructField(s.name, s.dataType) for s in schemaA]
        schB = [StructField(s.name, s.dataType) for s in schemaB]
  
    if (keepOrder == True):
      return [schA] == [schB]
    else:
      return set(schA) == set(schB)

# The label, expected number of cores and the expected DBR to use in conjunction with the call to valdiate_cluster()
validate_cluster_label = "Using DBR 9.1 & Proper Cluster Configuration"

def validate_cluster():
  import os
  expected_versions = ["9.1"]
  current_version = os.environ["DATABRICKS_RUNTIME_VERSION"]

  if current_version not in expected_versions:
    raise Exception(f"Expected DBR {expected_versions[0]} but found DBR {current_version}.<br/>Please reconfigure you cluster and try again.")
  elif sc.defaultParallelism not in [4,8]:
    raise Exception(f"Found DBR {expected_versions[0]}, but an incorrect cluster configuration.<br/>Please reconfigure you cluster with the correct Cluster Mode & Node Type and try again.")
  else:
    return True
    
def validate_registration_id(registration_id):
  try: 
    return BI.int(registration_id) > 0
  except: 
    return False
    
None # Suppress output

# COMMAND ----------

class DatabricksAcademyLogger:

  def logSuite(self, suiteName, registrationId, suite):
    self.logEvent(f"Test-{suiteName}.suite", f"""{{
      "registrationId": "{registrationId}", 
      "testId": "Suite-{suiteName}", 
      "description": "Suite level results",
      "status": "{"passed" if suite.passed else "failed"}",
      "actPoints": "{suite.score}", 
      "maxPoints": "{suite.maxScore}",
      "percentage": "{suite.percentage}"
    }}""")

  def logAggregatedResults(self, lessonName, registrationId, aggregator):
    self.logEvent(f"Lesson.final", f"""{{
      "registrationId": "{registrationId}", 
      "testId": "Aggregated-{lessonName}", 
      "description": "Aggregated results for lesson",
      "status": "{"passed" if aggregator.passed else "failed"}",
      "actPoints": "{aggregator.score}", 
      "maxPoints":   "{aggregator.maxScore}",
      "percentage": "{aggregator.percentage}"
    }}""")
    
  def logEvent(self, eventId: str, message: str = None):
    import time
    import json
    import requests
    
    hostname = "https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod"
    
    try:
      content = {
        "moduleName": "developer-foundations-capstone-v2",
        "lessonName": getLessonName(),
        "orgId":      getTag("orgId", "unknown"),
        "eventId":    eventId,
        "eventTime":  f"{BI.int(BI.round((time.time() * 1000)))}",
        "language":   getTag("notebookLanguage", "unknown"),
        "notebookId": getTag("notebookId", "unknown"),
        "sessionId":  getTag("sessionId", "unknown"),
        "message":    message
      }
      
      try:
        response = requests.post( 
            url=f"{hostname}/logger", 
            json=content,
            headers={
              "Accept": "application/json; charset=utf-8",
              "Content-Type": "application/json; charset=utf-8"
            })
        assert response.status_code == 200, f"Expected HTTP response code 200, found {response.status_code}"
        
      except requests.exceptions.RequestException as e:
        raise Exception("Exception sending message") from e
      
    except Exception as e:
      raise Exception("Exception constructing message") from e
    
daLogger = DatabricksAcademyLogger()

None # Suppress Output

# COMMAND ----------

# These imports are OK to provide for students
import pyspark
from typing import Callable, Any, Iterable, List, Set, Tuple
import uuid


#############################################
# Test Suite classes
#############################################

# Test case
class TestCase(object):
  __slots__=('description', 'testFunction', 'id', 'uniqueId', 'dependsOn', 'escapeHTML', 'points', 'hint')
  def __init__(self,
               description:str,
               testFunction:Callable[[], Any],
               id:str=None,
               dependsOn:Iterable[str]=[],
               escapeHTML:bool=False,
               points:int=1,
               hint=None):
    
    self.id=id
    self.hint=hint
    self.points=points
    self.escapeHTML=escapeHTML
    self.description=description
    self.testFunction=testFunction
    self.dependsOn = dependsOn if type(dependsOn) == list else [dependsOn]

# Test result
class TestResult(object):
  __slots__ = ('test', 'skipped', 'passed', 'status', 'points', 'exception', 'message')
  def __init__(self, test, skipped = False):
    try:
      self.test = test
      self.skipped = skipped
      if skipped:
        self.status = "skipped"
        self.passed = False
        self.points = 0
      else:
        assert test.testFunction() != False, "Test returned false"
        self.status = "passed"
        self.passed = True
        self.points = self.test.points
      self.exception = None
      self.message = ""
    except AssertionError as e:
      self.status = "failed"
      self.passed = False
      self.points = 0
      self.exception = e
      self.message = ""
    except Exception as e:
      self.status = "failed"
      self.passed = False
      self.points = 0
      self.exception = e
      self.message = str(e)

# Decorator to lazy evaluate - used by TestSuite
def lazy_property(fn):
    '''Decorator that makes a property lazy-evaluated.
    '''
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_property

  
testResultsStyle = """
<style>
  table { text-align: left; border-collapse: collapse; margin: 1em; caption-side: bottom; font-family: Sans-Serif; font-size: 16px}
  caption { text-align: left; padding: 5px }
  th, td { border: 1px solid #ddd; padding: 5px }
  th { background-color: #ddd }
  .passed { background-color: #97d897 }
  .failed { background-color: #e2716c }
  .skipped { background-color: #f9d275 }
  .results .points { display: none }
  .results .message { display: block; font-size:smaller; color:gray }
  .results .note { display: block; font-size:smaller; font-decoration:italics }
  .results .passed::before  { content: "Passed" }
  .results .failed::before  { content: "Failed" }
  .results .skipped::before { content: "Skipped" }
  .grade .passed  .message:empty::before { content:"Passed" }
  .grade .failed  .message:empty::before { content:"Failed" }
  .grade .skipped .message:empty::before { content:"Skipped" }
</style>
    """.strip()

# Test suite class
class TestSuite(object):
  def __init__(self) -> None:
    self.ids = set()
    self.testCases = list()

  @lazy_property
  def testResults(self) -> List[TestResult]:
    return self.runTests()
  
  def runTests(self) -> List[TestResult]:
    import re
    failedTests = set()
    testResults = list()
    
    for test in self.testCases:
      skip = any(testId in failedTests for testId in test.dependsOn)
      result = TestResult(test, skip)

      if (not result.passed and test.id != None):
        failedTests.add(test.id)

      if result.test.id: eventId = "Test-"+result.test.id 
      elif result.test.description: eventId = "Test-"+re.sub("[^a-zA-Z0-9_]", "", result.test.description).upper()
      else: eventId = "Test-"+str(uuid.uuid1())

      description = result.test.description.replace("\"", "'")
        
      message = f"""{{
        "registrationId": "{registration_id}", 
        "testId": "{eventId}", 
        "description": "{description}", 
        "status": "{result.status}", 
        "actPoints": "{result.points}", 
        "maxPoints": "{result.test.points}",
        "percentage": "{TestResultsAggregator.percentage}"
      }}"""

      daLogger.logEvent(eventId, message)

      testResults.append(result)
      TestResultsAggregator.update(result)
    
    return testResults

  def _display(self, cssClass:str="results") -> None:
    from html import escape
    lines = []
    lines.append(testResultsStyle)
    lines.append("<table class='"+cssClass+"'>")
    lines.append("  <tr><th class='points'>Points</th><th class='test'>Test</th><th class='result'>Result</th></tr>")
    
    for result in self.testResults:
      descriptionHTML = escape(str(result.test.description)) if (result.test.escapeHTML) else str(result.test.description)
      lines.append(f"<tr>")
      lines.append(f"  <td class='points'>{str(result.points)}</td>")
      lines.append(f"  <td class='test'>")
      lines.append(f"    {descriptionHTML}")
                   
      if result.status == "failed" and result.test.hint:
        lines.append(f"  <div class='note'>Hint: {escape(str(result.test.hint))}</div>")
        
      if result.message:
        lines.append(f"    <hr/>")
        lines.append(f"    <div class='message'>{str(result.message)}</div>")

      lines.append(f"  </td>")
      lines.append(f"  <td class='result {result.status}'></td>")
      lines.append(f"</tr>")
      
    lines.append("  <caption class='points'>Score: "+str(self.score)+"</caption>")
    lines.append("</table>")
    html = "\n".join(lines)
    displayHTML(html)
  
  def displayResults(self) -> None:
    self._display("results")
  
  def grade(self) -> int:
    self._display("grade")
    return self.score
  
  @lazy_property
  def score(self) -> int:
    return __builtins__.sum(map(lambda result: result.points, self.testResults))
  
  @lazy_property
  def maxScore(self) -> int:
    return __builtins__.sum(map(lambda result: result.test.points, self.testResults))

  @lazy_property
  def percentage(self) -> int:
    return 0 if self.maxScore == 0 else BI.int(100.0 * self.score / self.maxScore)

  @lazy_property
  def passed(self) -> bool:
    return self.percentage == 100

  def lastTestId(self) -> bool:
    return "-n/a-" if BI.len(self.testCases) == 0 else self.testCases[-1].id

  def addTest(self, testCase: TestCase):
    if not testCase.id: raise ValueError("The test cases' id must be specified")
    if testCase.id in self.ids: raise ValueError(f"Duplicate test case id: {testCase.id}")
    self.testCases.append(testCase)
    self.ids.add(testCase.id)
    return self
  
  def test(self, id:str, description:str, testFunction:Callable[[], Any], points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False, hint=None):
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points, hint=hint)
    return self.addTest(testCase)
  
  def testEquals(self, id:str, description:str, valueA, valueB, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False, hint=None):
    testFunction = lambda: valueA == valueB
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points, hint=hint)
    return self.addTest(testCase)
  
  def failPreReq(self, id:str, e:Exception, dependsOn:Iterable[str]=[]):
    self.fail(id=id, points=1, dependsOn=dependsOn, escapeHTML=False,
              description=f"""<div>Execute prerequisites.</div><div style='max-width: 1024px; overflow-x:auto'>{e}</div>""")
    
  def fail(self, id:str, description:str, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testCase = TestCase(id=id, description=description, testFunction=lambda: False, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
    
  def testFloats(self, id:str, description:str, valueA, valueB, tolerance=0.01, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: compareFloats(valueA, valueB, tolerance)
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)

  def testRows(self, id:str, description:str, rowA: pyspark.sql.Row, rowB: pyspark.sql.Row, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: compareRows(rowA, rowB)
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
  
  def testDataFrames(self, id:str, description:str, dfA: pyspark.sql.DataFrame, dfB: pyspark.sql.DataFrame, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: compareDataFrames(dfA, dfB)
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
  
  def testContains(self, id:str, description:str, listOfValues, value, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: value in listOfValues
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)

  
class __TestResultsAggregator(object):
  testResults = dict()
  
  def update(self, result:TestResult):
    self.testResults[result.test.id] = result
    return result
  
  @property
  def score(self) -> int:
    return __builtins__.sum(map(lambda result: result.points, self.testResults.values()))
  
  @property
  def maxScore(self) -> int:
    return __builtins__.sum(map(lambda result: result.test.points, self.testResults.values()))

  @property
  def percentage(self) -> int:
    return 0 if self.maxScore == 0 else BI.int(100.0 * self.score / self.maxScore)
  
  @property
  def passed(self) -> bool:
    return self.percentage == 100

  def displayResults(self):
    displayHTML(testResultsStyle + f"""
    <table class='results'>
      <tr><th colspan="2">Test Summary</th></tr>
      <tr><td>Number of Passing Tests</td><td style="text-align:right">{self.score}</td></tr>
      <tr><td>Number of Failing Tests</td><td style="text-align:right">{self.maxScore-self.score}</td></tr>
      <tr><td>Percentage Passed</td><td style="text-align:right">{self.percentage}%</td></tr>
    </table>
    """)
# Lazy-man's singleton
TestResultsAggregator = __TestResultsAggregator()  

None # Suppress Output

# COMMAND ----------

import re

# The user's name (email address) will be used to create a home directory into which all datasets will
# be written into so as to further isolating changes from other students running the same material.
username = getTag("user").lower()
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)

# The course dataset_name is simply the
# unique identifier for this project
# TODO - rename this to course_name
dataset_name = "developer-foundations-capstone"
clean_course_name = re.sub("[^a-zA-Z0-9]", "_", dataset_name)

# The path to our user's working directory. This combines both the
# username and dataset_name to create a "globally unique" folder
working_dir = f"dbfs:/user/{username}/dbacademy/{dataset_name}"
meta_dir = f"{working_dir}/raw/orders/batch/2017.txt"

batch_2017_path = f"{working_dir}/raw/orders/batch/2017.txt"
batch_2018_path = f"{working_dir}/raw/orders/batch/2018.csv"
batch_2019_path = f"{working_dir}/raw/orders/batch/2019.csv"
batch_target_path = f"{working_dir}/batch_orders_dirty.delta"

stream_path =                        f"{working_dir}/raw/orders/stream"
orders_checkpoint_path =             f"{working_dir}/checkpoint/orders"
line_items_checkpoint_path =         f"{working_dir}/checkpoint/line_items"

products_xsd_path = f"{working_dir}/raw/products/products.xsd"
products_xml_path = f"{working_dir}/raw/products/products.xml"

batch_source_path = batch_target_path
batch_temp_view = "batched_orders"

user_db = f"""dbacademy_{clean_username}_{clean_course_name}"""

orders_table = "orders"
sales_reps_table = "sales_reps"
products_table = "products"
#product_line_items_table = "product_line_items"
line_items_table = "line_items"

question_1_results_table = "question_1_results"
question_2_results_table = "question_2_results"
question_3_results_table = "question_3_results"

def load_meta():
  global meta, meta_batch_count_2017, meta_batch_count_2018, meta_batch_count_2019, meta_products_count, meta_orders_count, meta_line_items_count, meta_sales_reps_count, meta_stream_count, meta_ssn_format_count
  
  meta = spark.read.json(f"{working_dir}/raw/_meta/meta.json").first()
  meta_batch_count_2017 = meta["batchCount2017"]
  meta_batch_count_2018 = meta["batchCount2018"]
  meta_batch_count_2019 = meta["batchCount2019"]
  meta_products_count = meta["productsCount"]
  meta_orders_count = meta["ordersCount"]
  meta_line_items_count = meta["lineItemsCount"]
  meta_sales_reps_count = meta["salesRepsCount"]
  meta_stream_count = meta["streamCount"]
  meta_ssn_format_count = meta["ssnFormatCount"]

