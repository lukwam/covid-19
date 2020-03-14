"""Cloud Function to move GitHub to Bucket."""

import base64
import google.auth
import json

from github import Github
from google.cloud import storage
from google.cloud import secretmanager_v1


def _get_github_token():
    """Return the github token from secret manager."""
    _, project = google.auth.default()
    client = secretmanager_v1.SecretManagerServiceClient()
    name = client.secret_version_path(project, 'github-token', 'latest')
    response = client.access_secret_version(name)
    return response.payload.data.decode('utf8')


def _get_repo_contents(repo):
    """Return a list of the files in the repo."""
    files = []
    contents = repo.get_contents("")
    while contents:
        file_content = contents.pop(0)
        if file_content.type == "dir":
            contents.extend(repo.get_contents(file_content.path))
        else:
            files.append(file_content)
    return files


def _save_files_to_bucket(files):
    """Save files to Google Cloud Storage bucket."""
    storage_client = storage.Client()
    bucket_name = 'broad-covid-19'
    bucket = storage_client.bucket(bucket_name)
    for file_content in files:
        # set file metadata
        metadata = {
            'download_url': file_content.download_url,
            'git_url': file_content.git_url,
            'html_url': file_content.html_url,
            'name': file_content.name,
            'path': file_content.path,
            'sha': file_content.sha,
            'size': str(file_content.size),
            'url': file_content.url,
        }

        # get current blob
        blob = bucket.get_blob(file_content.path)

        # upload content if file does not exist
        if not blob:
            print('Adding file: {}'.format(file_content.path))
            blob = bucket.blob(file_content.path)
            _upload_file_to_gcs(blob, bucket_name, file_content, metadata)

        # otherwise, check if file needs to be uploaded
        elif blob.metadata != metadata:
            print('Updating file: {}'.format(file_content.path))
            _upload_file_to_gcs(blob, bucket_name, file_content, metadata)


def _upload_file_to_gcs(blob, bucket_name, file_content, metadata):
    """Upload a single file to GCS."""
    blob.metadata = metadata
    file_string = base64.b64decode(file_content.content).decode('utf-8')
    blob.upload_from_string(file_string, content_type='text/csv')
    print("File {} uploaded to gs://{}/{}.".format(
        file_content.path,
        bucket_name,
        file_content.path,
    ))


def github_to_bucket(request):
    """Move GitHub CSV to Bucket."""
    # get github token from secret manager
    token = _get_github_token()

    # authenticate to github
    github = Github(token)

    # get jhu csse covid-19 repo
    repo = github.get_repo('CSSEGISandData/COVID-19')

    # get repo contents
    files = _get_repo_contents(repo)

    csse_covid_19_daily_reports = []
    csse_covid_19_time_series = []
    who_covid_19_sit_rep_time_series = []

    # get csvs from contents
    for file_content in files:
        if not file_content.path.endswith('.csv'):
            continue
        elif 'csse_covid_19_daily_reports' in file_content.path:
            csse_covid_19_daily_reports.append(file_content)
        elif 'csse_covid_19_time_series' in file_content.path:
            csse_covid_19_time_series.append(file_content)
        elif 'who_covid_19_sit_rep_time_series' in file_content.path:
            who_covid_19_sit_rep_time_series.append(file_content)

    # save files to GCS
    _save_files_to_bucket(csse_covid_19_daily_reports)
    _save_files_to_bucket(csse_covid_19_time_series)
    _save_files_to_bucket(who_covid_19_sit_rep_time_series)


if __name__ == '__main__':
    github_to_bucket(None)
