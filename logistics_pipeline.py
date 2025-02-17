import argparse
import time
import logging
import json
import typing
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.runners import DataflowRunner, DirectRunner

# ### functions and classes

class LogisticsEvent(typing.NamedTuple):
    event_id: str
    order_id: str
    product_id: str
    status: str
    event_timestamp: str
    warehouse_id: str

beam.coders.registry.register_coder(LogisticsEvent, beam.coders.RowCoder)

def parse_json(element):
    row = json.loads(element.decode('utf-8'))
    # Only keep the fields we need
    required_fields = {
        'event_id': row['event_id'],
        'order_id': row['order_id'],
        'product_id': row['product_id'],
        'status': row['status'],
        'event_timestamp': row['event_timestamp'],
        'warehouse_id': row['warehouse_id']
    }
    return LogisticsEvent(**required_fields)

def add_processing_timestamp(element):
    row = element._asdict()
    row['event_timestamp'] = row.pop('event_timestamp')
    row['processing_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return row

class GetTimestampFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime()
        window_end = window.end.to_utc_datetime()
        output = {
            'window_start': window_start,
            'window_end': window_end,
            'status': element[0],  # status
            'count': element[1],   # count
            'avg_processing_time': 0.0  # placeholder for future implementation
        }
        yield output

# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--staging_location', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--temp_location', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--input_topic', required=True, help='Input Pub/Sub Topic')
    parser.add_argument('--agg_table_name', required=True, help='BigQuery table name for aggregate results')
    parser.add_argument('--raw_table_name', required=True, help='BigQuery table name for raw inputs')
    parser.add_argument('--window_duration', required=True, help='Window duration in seconds')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(save_main_session=True, streaming=True)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('logistics-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    input_topic = opts.input_topic
    raw_table_name = opts.raw_table_name
    agg_table_name = opts.agg_table_name
    window_duration = int(opts.window_duration)

    # Table schema for BigQuery
    raw_table_schema = {
        "fields": [
            {"name": "event_id", "type": "STRING"},
            {"name": "order_id", "type": "STRING"},
            {"name": "product_id", "type": "STRING"},
            {"name": "status", "type": "STRING"},
            {"name": "event_timestamp", "type": "STRING"},
            {"name": "processing_timestamp", "type": "STRING"},
            {"name": "warehouse_id", "type": "STRING"}
        ]
    }

    agg_table_schema = {
        "fields": [
            {"name": "window_start", "type": "TIMESTAMP"},
            {"name": "window_end", "type": "TIMESTAMP"},
            {"name": "status", "type": "STRING"},
            {"name": "count", "type": "INTEGER"},
            {"name": "avg_processing_time", "type": "FLOAT"}
        ]
    }

    # Create the pipeline
    p = beam.Pipeline(options=options)

    # Read from PubSub and parse messages
    parsed_msgs = (p | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(input_topic)
                    | 'ParseJson' >> beam.Map(parse_json).with_output_types(LogisticsEvent))

    # Write raw events to BigQuery
    (parsed_msgs 
        | "AddProcessingTimestamp" >> beam.Map(add_processing_timestamp)
        | 'WriteRawToBQ' >> beam.io.WriteToBigQuery(
            raw_table_name,
            schema=raw_table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
    )

    # Aggregate events by status and window
    (parsed_msgs
        | "ExtractStatus" >> beam.Map(lambda x: (x.status, x))
        | "WindowByMinute" >> beam.WindowInto(beam.window.FixedWindows(window_duration))
        | "CountPerStatus" >> beam.CombinePerKey(beam.transforms.combiners.CountCombineFn())
        | "AddWindowTimestamp" >> beam.ParDo(GetTimestampFn())
        | 'WriteAggToBQ' >> beam.io.WriteToBigQuery(
            agg_table_name,
            schema=agg_table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
    run()