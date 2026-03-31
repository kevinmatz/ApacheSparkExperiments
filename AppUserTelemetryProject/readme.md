# App User Telemetry Project: A PySpark feature pipeline for user behavior analysis

app_user_telemetry_spark_pipeline.py is a small feature-engineering pipeline using Apache Spark and PySpark. It transforms raw app event data (currently just reading from a .csv file) from a fictional e-commerce site into a user-level feature table that can be used for training a downstream machine learning model to predict user behavior (e.g., likelihood of purchase).

The generate_events.py file can be used to generate a sample events.csv file with fake data for testing this pipeline:

event_id,user_id,event_type,product_id,timestamp,price,device_type,country
59768816-7296-45c4-b78d-92a1156c9810,user_655,view,product_95,2026-03-12T06:47:21.780186,,mobile,US
06b26f36-7f3d-4257-97e6-9145e2937867,user_143,click,product_87,2026-03-24T14:04:21.780186,,mobile,UK

Note: Warnings on Windows about "winutils.exe", HADOOP_HOME, and hadoop.home.dir can be ignored.

Kevin Matz, 2026-03-31


## How to run

On Windows:

* .\start_venv.bat
* python generate_events.py
  * This generates an events.csv file
* .\run.ps1
  * Consumes the events.csv file and outputs feature tables in CSV and Parquet formats under the output/ directory


## What the Spark pipeline does

1. Reads data from the CSV file into a Spark DataFrame
2. Cleans and standardizes the data (omits duplicate events, drops rows missing critical values, replaces null price values with 0.0, etc.)
3. Creates four columns in the DataFrame representing binary features indicating what type of event each data row represents
4. A features DataFrame is created by aggregating, per user_id, values for total number of views, total number of clicks, total number of "add to cart" events, total purchases, total spent, average order value, etc.
5. Export features table to CSV and Parquet

Further enhancements:

* TODO: Revise pipeline to use a time split for supervised learning: e.g., if the raw data spans 30 days, use days 1-23 for features, and 24-30 for labels for training
  * Label could be "did the user make a purchase in the next 7 days" (boolean)
* TODO: Partition the output by country
* TODO: Export features to pandas a train a logistic regression (binary classification)



## Installing Apache Spark and setting up an appropriate venv on Windows 11

* After much troubleshooting, it turns out that Spark 4.1.1 does not run at all with Java 25 or Python 3.13.7
* Install Java 21
* I already had a Python 3.10.6 instance already installed, so I'll use that for now although 3.11 is supported and would be better
* Make sure there are no environment variables set for %SPARK_HOME%, %PYTHONPATH%, or %HADOOP_HOME% as these will cause endless problems

* To create an appropriate venv:
  * cd C:\GitRepos\ApacheSparkExperiments\AppUserTelemetryProject
  * C:\DevTools\Python310\python.exe -m venv venv-py3.10.6
  * .\venv-py3.10.6\Scripts\Activate.ps1
  * python -m pip install --upgrade pip
  * python -m pip install pyspark==4.1.1
  * pip freeze > requirements.txt
  * Test/check:
    * $env:PYSPARK_PYTHON = "$PWD\venv-py3.10.6\Scripts\python.exe"
    * $env:PYSPARK_DRIVER_PYTHON = "$PWD\venv-py3.10.6\Scripts\python.exe"
    * python .\test_spark.py
  * Reminder: "deactivate" to exit the venv in the shell

* Spark seems to fail in various ways (e.g., DataFrame.show() not working and various spurious error messages) if the following lines are not included:

```
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
```


## Working around failure of Hadoop winutils.exe on Windows

* Download .jar file: https://repo1.maven.org/maven2/com/globalmentor/hadoop-bare-naked-local-fs/0.1.0/hadoop-bare-naked-local-fs-0.1.0.jar
* Put .jar file in C:\GitRepos\ApacheSparkExperiments\AppUserTelemetryProject\jars\hadoop-bare-naked-local-fs-0.1.0.jar
* In PowerShell, run command:

$env:PYSPARK_SUBMIT_ARGS='--driver-class-path "C:\GitRepos\ApacheSparkExperiments\AppUserTelemetryProject\jars\hadoop-bare-naked-local-fs-0.1.0.jar" --conf "spark.executor.extraClassPath=C:\GitRepos\ApacheSparkExperiments\AppUserTelemetryProject\jars\hadoop-bare-naked-local-fs-0.1.0.jar" pyspark-shell'

before running

python .\app_user_telemetry_spark_pipeline.py

* The above commands have been consolidated into .\run.ps1


## Notes on previous attempt to install bypassing "pip install pyspark" (as per online advice)

* "pip install pyspark" assumes you have an existing cluster and won't create/install a server, so instead see: https://spark.apache.org/docs/latest/api/python/getting_started/install.html 
* Download .tar.gz from https://spark.apache.org/downloads.html
* If unpacked to C:\DevTools\spark-4.1.1-bin-hadoop3-connect, then set environment variables as follows:
  * %SPARK_HOME% = C:\DevTools\spark-4.1.1-bin-hadoop3-connect
  * %PYTHONPATH% = C:\DevTools\spark-4.1.1-bin-hadoop3-connect\python\lib\pyspark.zip;C:\DevTools\spark-4.1.1-bin-hadoop3-connect\python\lib\py4j-0.10.9.9-src.zip
* Also Java 17+ is required (UPDATE: but JDK 25 is NOT supported) and make sure that JAVA_HOME is set, for example:
  * %JAVA_HOME% = C:\DevTools\AdoptiumJDK\jdk-21.0.10+7
* Troubleshooting errors:
  * Java 25 is not supported -- this was causing "UnsupportedOperationException: getSubject is not supported" errors which are apparently a known issue
  * Need to install JDK 17 or JDK 21 instead
  * Got error/warning when trying to run Apache Spark locally:
    * "26/03/30 14:13:54 WARN Shell: Did not find winutils.exe: java.io.FileNotFoundException: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset. -see https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems"
  * See
    * https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems
    * https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin
    * Downloaded winutils.exe and put in C:\DevTools\HadoopPlaceholder\bin
    * Then set %HADOOP_HOME%=C:\DevTools\HadoopPlaceholder
* Abandoned this path and went back to "pip install pyspark" approach
