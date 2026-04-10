# Clickstream (App User Telemetry) Project: A demo Kafka + PySpark feature pipeline for user behavior analysis

This project is a proof-of-concept of a small feature-engineering pipeline using Apache Spark and PySpark, using Apache Kafka as a data source.

The pipeline consumes events from a Kafka topic, and transforms raw app event data in JSON lines format, representing user activities on a fictional e-commerce app/site, into user-level feature tables that can be used for training a downstream machine learning model to predict user behavior (e.g., likelihood of purchase). 

* **test_data_creation/generate_test_events_json_lines.py** creates a test data file **events.jsonl**
* Kafka runs in a Docker container running locally using Docker Desktop (see instructions below), with a single topic *app-user-events*
* **test_data_producer_to_kafka.py** reads the **events.jsonl** test data file and publishes them to the Kafka topic
* **clickstream_spark_pipeline_from_kafka.py** is the Spark pipeline that consumes events from the Kafka topic, and cleans and transforms the data into features for machine learning model training; it currently saves output batches every 10 seconds in Parquet and CSV formats under the **output/** directory in the local filesystem

Below are instructions on how to set up the environment and run the project.

This version is set up to run under WSL2 in Windows with Java JDK 21, Docker Desktop configured to use WSL2, Python 3.11.9, and a venv with PySpark / Spark 4.1.1.

IMPORTANT: Apache Spark 4.1.1 is NOT compatible with Python 3.13 or with Java 25.

Kevin Matz, 2026-03-31

Acknowledgement: Assistance from OpenAI Codex and ChatGPT


## Further planned enhancements / next steps

* TODO: Revise pipeline to use a time split for supervised learning: e.g., if the raw data spans 30 days, use days 1-23 for features, and 24-30 for labels for training
  * Label could be "did the user make a purchase in the next 7 days" (boolean)
* TODO: Partition the output by country
* TODO: Export features to pandas a train a logistic regression (binary classification)


## Initial setup prerequisites: WSL2 on Windows 11, installing correct version of Python, creating project venv, and installing Apache Spark

* Install WSL if not already set up; in PowerShell:
  * wsl --install
* In WSL, initial setup:
  * sudo apt update
  * sudo apt install python3.11 python3.11-venv python3.11-dev
  * sudo apt install -y build-essential curl git libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev libffi-dev liblzma-dev tk-dev
  * curl https://pyenv.run | bash
  * nano ~/.bashrc
  * Add lines:
    * export JAVA_HOME=/usr/lib/jvm/java-21-open jdk-amd64
    * export PATH=$JAVA_HOME/bin:$PATH
  * source ~/.bashrc
  * pyenv install 3.11.9
  * pyenv global 3.11.9
  * python --version
    * Check: 3.11.9
  * cd /mnt/c/GitRepos/ApacheSparkExperiments/AppUserTelemetryProject  (on my system)
  * python -m venv venv_3.11.9_WSL
  * source ./venv_3.11.9_WSL/bin/activate
  * python -m pip install --upgrade pip
  * python -m pip install pyspark==4.0.1
    * UPDATE: Initially I was using pyspark==4.1.1 but ran into known bug SPARK-55271 "NullPointerException in Kafka Micro-Batch Streaming Progress Reporting" with 4.1.1, so need to downgrade to 4.0.1 for now
  * pip install confluent-kafka
  * pip freeze > requirements.txt
* Ensure Docker Desktop is installed; under Settings, ensure it is configured to work with WSL2 and Ubuntu


## How to run the demo project

In a first WSL Ubuntu window / bash shell:

* cd /mnt/c/GitRepos/ApacheSparkExperiments/AppUserTelemetryProject 
* source ./venv_3.11.9_WSL/bin/activate
* Generate test data:
  * python ./test_data_creation/generate_test_events_json_lines.py
  * This creates a file "events.jsonl"
* Start the Kafka Docker container and create a topic "app-user-events":
  * docker run -d --name kafka_container -p 9092:9092 apache/kafka:4.2.0
  * docker exec -it kafka_container /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic app-user-events --partitions 1 --replication-factor 1
* Start the tool that produces events that are sent to the Kafka topic:
  * python ./test_data_producer_to_kafka.py

To demonstrate consumption of the Kafka events, in a separate Ubuntu window / bash shell:

* cd /mnt/c/GitRepos/ApacheSparkExperiments/AppUserTelemetryProject  (on my system)
* source ./venv_3.11.9_WSL/bin/activate
* Start the Spark pipeline (consumer of events from the Kafka topic):
  * python ./clickstream_spark_pipeline_from_kafka.py

To train a logistic regression model (for now, a one-time event -- later iterations of the project will trigger retraining periodically):

* cd one_off_model_training
* python ./logistic_regression_sklearn_model_one_off.py
  * NOTE: This has hardcoded date ranges for now
  * This creates <project_root>/purchase_model.joblib -- a saved copy of the model
  * Console output shows accuracy with the training/test split data, and shows a demo of inference using a single sample feature vector (output: prediction and probability)
* cd ..
  * TODO: Project structure may be revised

To see a demo of live inference scoring from Kafka Stream with Spark Structured Streaming and scikit-learn logistic regression model:

* In one Ubuntu window/shell, start the tool that produces events that are sent to the Kafka topic:
  * python ./test_data_producer_to_kafka.py
* In another Ubuntu window/shell:
  * python ./live_inference_scoring_from_kafka.py

Reminder: "deactivate" to exit the venv in the shell
