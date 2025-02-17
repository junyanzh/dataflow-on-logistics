# Logistics Events Pipeline PoC

This repository provides a complete proof-of-concept (PoC) for simulating and processing logistics events on Google Cloud Platform (GCP). It includes the necessary scripts and code to generate random events, process them using an Apache Beam pipeline on Dataflow, and store both raw and aggregated data in BigQuery.

## Repository Overview

- **Event Generator**  
  - *File:* `event_generator.py`  
  Generates random logistics events (e.g., order creation, stock checking, shipping, delivery) and publishes them to a Pub/Sub topic.

- **Dataflow Pipeline**  
  - *File:* `logistics_pipeline.py`  
  Defines an Apache Beam pipeline that reads events from Pub/Sub, processes them (e.g., adds timestamps, performs windowing/aggregation), and writes both raw and aggregated data to BigQuery.

- **Environment Setup**  
  - *File:* `create_streaming_sinks.sh`  
  Sets up the required GCP resources such as a GCS bucket, BigQuery dataset/tables, and Pub/Sub topic.

## Usage

- **Setting Up GCP Resources**: Refer to the `create_streaming_sinks.sh` script for the commands to create your GCP resources.  
- **Running the Pipeline**: Refer to the documentation or scripts (e.g., `pipeline_run.sh`) for detailed instructions and additional parameters. Below is an example snippet in a single code block:

```bash
export PROJECT_ID=$(gcloud config get-value project)
export REGION='us-west1'
export BUCKET=gs://${PROJECT_ID}
export PIPELINE_FOLDER=${BUCKET}
export RUNNER=DataflowRunner
export PUBSUB_TOPIC=projects/${PROJECT_ID}/topics/logistics_events
export WINDOW_DURATION=60
export AGGREGATE_TABLE_NAME=${PROJECT_ID}:logistics_data.logistics_metrics
export RAW_TABLE_NAME=${PROJECT_ID}:logistics_data.logistics_events

python3 logistics_pipeline.py \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --staging_location=${PIPELINE_FOLDER}/staging \
    --temp_location=${PIPELINE_FOLDER}/temp \
    --runner=${RUNNER} \
    --input_topic=${PUBSUB_TOPIC} \
    --window_duration=${WINDOW_DURATION} \
    --agg_table_name=${AGGREGATE_TABLE_NAME} \
    --raw_table_name=${RAW_TABLE_NAME}
```




- Generating Events: Refer to event_generator.py and related documentation for instructions on installing dependencies and running the event generator.

- Verifying in BigQuery: SQL queries for verification are available in the corresponding documentation or comments within the code.

## Summary
In this PoC, you can generate, process, and aggregate logistics events using GCP services. For more details, explore the following files:

- **event_generator.py**
- **logistics_pipeline.py**
- **create_streaming_sinks.sh**

  
Feel free to modify the code to fit your own use cases!
