# -*- coding: utf-8 -*-
"""Cloud Function to move Bucket data to BigQuery."""

import csv
import datetime
import io
import re

from google.cloud import bigquery
from google.cloud import storage


def _load_into_bigquery():
    """Load combined CSV into bigquery."""
    dataset_id = 'csse'
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_name = 'daily_reports'
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("Country_Region", "STRING"),
        bigquery.SchemaField("Province_State", "STRING"),
        bigquery.SchemaField("Confirmed", "INTEGER"),
        bigquery.SchemaField("Deaths", "INTEGER"),
        bigquery.SchemaField("Recovered", "INTEGER"),
        bigquery.SchemaField("Last_Update", "DATETIME"),
        bigquery.SchemaField("Latitude", "FLOAT"),
        bigquery.SchemaField("Longitude", "FLOAT"),
    ]
    job_config.autodetect = True
    job_config.create_disposition = 'CREATE_IF_NEEDED',
    # job_config.source_format = 'NEWLINE_DELIMITED_JSON',
    job_config.write_disposition = 'WRITE_TRUNCATE',
    job_config.skip_leading_rows = 1

    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    uri = "gs://broad-covid-19-bigquery/daily_reports.csv"

    load_job = client.load_table_from_uri(
        uri, dataset_ref.table(table_name), job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table(table_name))
    print("Loaded {} rows.".format(destination_table.num_rows))


def _save_bigquery_csv(daily_reports):
    """Save combined CSV to GCS."""
    output = io.StringIO()
    writer = csv.writer(output)
    for report in daily_reports:
        writer.writerow(report)

    bucket_name = 'broad-covid-19-bigquery'
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob('daily_reports.csv')
    blob.upload_from_string(output.getvalue(), content_type='text.csv')


def bucket_to_bigquery(request):
    """Move GitHub CSV to Bucket."""
    bucket_name = 'broad-covid-19'
    client = storage.Client()
    header = [
        "Country/Region",
        "Province/State",
        "Confirmed",
        "Deaths",
        "Recovered",
        "Last Update",
        "Latitude",
        "Longitude",
    ]

    daily_reports = [header]
    for blob in client.list_blobs(bucket_name):
        if 'csse_covid_19_daily_reports' in blob.name:
            blob_string = blob.download_as_string().decode('utf-8-sig')
            reader = csv.DictReader(io.StringIO(blob_string))
            for row in sorted(reader, key=lambda x: x['Last Update']):
                last_update = row.get("Last Update")
                if re.match(r'[0-9]+/[0-9]+/[0-9]{4} ', last_update):
                    last_update = datetime.datetime.strptime(
                        last_update,
                        '%m/%d/%Y %H:%M',
                    )
                elif re.match(r'[0-9]+/[0-9]+/[0-9]{2} ', last_update):
                    last_update = datetime.datetime.strptime(
                        last_update,
                        '%m/%d/%y %H:%M',
                    )
                else:
                    last_update = datetime.datetime.strptime(
                        last_update,
                        '%Y-%m-%dT%H:%M:%S',
                    )
                report = [
                    row.get("Country/Region"),
                    row.get("Province/State"),
                    row.get("Confirmed"),
                    row.get("Deaths"),
                    row.get("Recovered"),
                    last_update,
                    row.get("Latitude"),
                    row.get("Longitude"),
                ]
                daily_reports.append(report)

    print('Daily Reports: {}'.format(len(daily_reports)))
    _save_bigquery_csv(daily_reports)
    _load_into_bigquery()


if __name__ == '__main__':
    bucket_to_bigquery(None)
