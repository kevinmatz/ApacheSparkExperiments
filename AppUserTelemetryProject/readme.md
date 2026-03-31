# Installing Apache Spark and setting up an appropriate venv on Windows 11

* After much troubleshooting, it turns out that Spark 4.1.1 does not run at all with Java 25 or Python 3.13.7
* Install Java 21
* I already had a Python 3.10.6 instance already installed, so I'll use that for now although 3.11 is supported and would be better
* Make sure there are no environment variables set for %SPARK_HOME%, %PYTHONPATH%, or %HADOOP_HOME% as these will cause endless problems

* To create an appropriate venv:
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
