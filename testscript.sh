#!/bin/bash

echo "Create Partition BigQuery table"
echo ""

#export GOOGLE_APPLICATION_CREDENTIALS="/Users/apani/repo/Manning/GCPSvcCredentials/manning-data-pipelines-082a4f59df18.json"

export PROJECT_ID=manning-data-pipelines
export PIPELINE_FOLDER=gs://manning-data-pipelines-temp/dataflow/pipelines/pubsub-to-bigquery
export REGION_ID=us-central1
export BUCKET_NAME=${PROJECT_ID}-dataflow-templates
 *
 * # Set the runner
export RUNNER=DataflowRunner

####--serviceAccountName=paniaroni@manning-data-pipelines.gserviceaccount.com \
##~/User/user/software/apache-maven-3.8.1/bin/mvn compile exec:java \
mvn compile exec:java \
-Dexec.mainClass=com.beam.pipeline.myapp.LoadToBQFromFeed  \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=${PROJECT_ID} \
--region=${REGION_ID} \
--stagingLocation=${PIPELINE_FOLDER}/staging \
--tempLocation=${PIPELINE_FOLDER} \
--runner=${RUNNER} \gcloud beta dataflow jobs list --status=active
--subscription=projects/${PROJECT_ID}/topics/data-streaming-topic \
--outputTableDataset=${PROJECT_ID}:tsunami.telemetry_data_partitioned \
--streaming=true \
--triggerMaxHeight=30.0"