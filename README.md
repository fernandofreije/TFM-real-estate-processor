# TFM-real-estate-processor

![{{alt text}}](https://github.com/fernandofreije/TFM-real-estate-processor/workflows/CI%20Upload%20Script/badge.svg)

Pyspark ETL to process real estate data

This project have two Github Actions that both:

- Deploy the files to a Google Cloud Storage
- Create a job in Google Cloud Dataproc

## How to run locally

Pull the docker image

```bash
docker pull godatadriven/pyspark
```

Run the job

```bash
docker run --rm -v $(pwd):/job godatadriven/pyspark --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 /job/src/process_real_estate.py
```

## How to setup Dataproc

### Setup a Dataproc workflow

Example:

```bash
gcloud dataproc workflow-templates create {name-of-the-workflow} --region europe-west2
```

Real line:

```bash
gcloud dataproc workflow-templates create real-estate-processing-workflow --region europe-west2
```

### Setup a managed cluster with to create with that workflow

Minimun cluster example:

```bash
gcloud beta dataproc workflow-templates set-managed-cluster real-estate-processing-workflow --region europe-west2 --master-machine-type n1-standard-1 --master-boot-disk-size 500 --num-workers 2 --worker-machine-type n1-standard-1 --worker-boot-disk-size 100 --cluster-name spark-notebook  --initialization-actions=gs://real_estate_spark_processing_bucket/init_actions_cluster.bash
```

Standard cluster example:

```bash
gcloud beta dataproc workflow-templates set-managed-cluster real-estate-processing-workflow --region europe-west2 --cluster-name spark-notebook  --initialization-actions=gs://real_estate_spark_processing_bucket/init_actions_cluster.bash
```

### Create a job based on the workflow template

In this case there are some added lines (for example the `init_actions_cluster.bash` execution to setup Python3)

```bash
gcloud dataproc workflow-templates add-job pyspark --properties spark.jars.packages=org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 gs://real_estate_spark_processing_bucket/process_real_estate.py --step-id summary-processing --workflow-template real-estate-processing-workflow --region europe-west2
```

### Run the job

```bash
gcloud dataproc workflow-templates instantiate real-estate-processing-workflow --region=europe-west2
```
