# Logistics Events Pipeline PoC

This repository demonstrates a complete end-to-end proof-of-concept (PoC) for simulating and processing logistics events in a cloud environment using Google Cloud Platform services.

## Overview

The project consists of two main components:

- **Event Generator**  
  The event generator simulates logistics activities by randomly creating events (e.g., order creation, stock checking, shipping, and delivery). These events are published to Google Cloud Pub/Sub.  
  *File:* [event_generator.py](./event_generator.py)

- **Dataflow Pipeline**  
  The Apache Beam pipeline reads the events from Pub/Sub, processes and transforms the data (including adding processing timestamps and aggregating events within fixed time windows), and writes both raw events and aggregated metrics into BigQuery.  
  *File:* [logistics_pipeline.py](./logistics_pipeline.py)

## Pipeline Components

### Pipeline Files

- **logistics_pipeline.py**  
  Contains the main Dataflow pipeline code, which reads from Pub/Sub, processes the events, and writes results to BigQuery.

- **event_generator.py**  
  Contains the code to generate logistics events and publish them to the Pub/Sub topic.

### Environment Setup Script

The repository includes the following bash script to set up the required GCP resources:

```bash
#!/usr/bin/env bash
echo "Creating pipeline sinks"

PROJECT_ID=$(gcloud config get-value project)

# GCS bucket
gsutil mb -l US gs://$PROJECT_ID

# BigQuery Dataset
bq mk --location=US logistics_data

# Create tables schema
bq mk --table \
  logistics_data.logistics_events \
  event_id:STRING,order_id:STRING,product_id:STRING,status:STRING,event_timestamp:STRING,processing_timestamp:STRING,warehouse_id:STRING

bq mk --table \
  logistics_data.logistics_metrics \
  window_start:TIMESTAMP,window_end:TIMESTAMP,status:STRING,count:INTEGER,avg_processing_time:FLOAT

# PubSub Topic
gcloud pubsub topics create logistics_events
