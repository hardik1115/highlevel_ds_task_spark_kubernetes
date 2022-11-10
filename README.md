# highlevel_ds_task_spark_kubernetes

The files are as follows; 

1. Worker Dockerfile, 
2. Script to read, clean, process and model on the housing data.
3. Cluster source and configs.

The set-up is as follows: Spark is set up on Kubernetes running with Docker Engine. A job is submitted using the command snippet below, it spins up worker pods as 'n' spark.executor.instances or nodes in a cluster mode, 'spark:spark-docker' is the image used to spin up these worker instances. The dependencies and scripts as in the zipped cluster source.

Used mlflow to log and track training and validation metrics, training time, r2, mse.  

The dockerfile has been edited at spark>kubernetes>dockerfiles>spark and the edited one is uploaded here.

To submit a job, run: 

bin/spark-submit  --master k8s://https://localhost:6443 --deploy-mode cluster --conf spark.executor.instances=1      --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark      --conf spark.kubernetes.container.image=spark:spark-docker --name spark-pi --driver-memory 1g --executor-memory 2g --executor-cores 2 local:///opt/spark/examples/src/main/python/test_script_pyspark.py

