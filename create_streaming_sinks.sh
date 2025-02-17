#!/usr/bin/env bash
echo "Creating pipeline sinks"

PROJECT_ID=$(gcloud config get-value project)

# GCS buckets
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
