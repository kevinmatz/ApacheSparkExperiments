$env:PYSPARK_SUBMIT_ARGS='--driver-class-path "C:\GitRepos\ApacheSparkExperiments\AppUserTelemetryProject\jars\hadoop-bare-naked-local-fs-0.1.0.jar" --conf "spark.executor.extraClassPath=C:\GitRepos\ApacheSparkExperiments\AppUserTelemetryProject\jars\hadoop-bare-naked-local-fs-0.1.0.jar" pyspark-shell'
python .\spark_feature_pipeline1.py
