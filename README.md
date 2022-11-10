# highlevel_ds_task_spark_kubernetes

The files are as follows; 

1. Worker Dockerfile, 
2. Script to read, clean, process and model on the housing data.
3. Cluster source and configs; https://drive.google.com/file/d/1vT0uX8AiPSTQiYhlK5M5wylqrwhVkZoD/view?usp=sharing

The set-up is as follows: Spark is set up on Kubernetes running with Docker Engine. A job is submitted using the command snippet below, it spins up worker pods as 'n' spark.executor.instances or nodes in a cluster mode, 'spark:spark-docker' is the image used to spin up these worker instances. The dependencies and scripts as in the zipped cluster source.

Used mlflow to log and track training and validation metrics, training time, r2, mse.  

The dockerfile has been edited at spark>kubernetes>dockerfiles>spark and the edited one is uploaded here.

To submit a job, run: 

bin/spark-submit  --master k8s://https://localhost:6443 --deploy-mode cluster --conf spark.executor.instances=1      --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark      --conf spark.kubernetes.container.image=spark:spark-docker --name spark-pi --driver-memory 1g --executor-memory 2g --executor-cores 2 local:///opt/spark/examples/src/main/python/test_script_pyspark.py

Quick summary of results: 

1 executor: 

MAE: 12721.646279
r2: 0.986808
Training time: 69.5273814201355s

2 Executors: 

MAE: 12721.646279
r2: 0.986808
Training time: 38.384032249450684s

4 Executors:

MAE: 12721.646279
r2: 0.986808
Training time: 35.335444688797s
