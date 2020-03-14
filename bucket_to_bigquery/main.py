# -*- coding: utf-8 -*-
"""Cloud Function to move Bucket data to BigQuery."""

# import google.auth

# from google.cloud import bigquery
from google.cloud import storage


def bucket_to_bigquery(request):
    """Move GitHub CSV to Bucket."""
    bucket_name = 'broad-covid-19'
    client = storage.Client()
    for blob in client.list_blobs(bucket_name):
        print(blob)


if __name__ == '__main__':
    bucket_to_bigquery(None)
