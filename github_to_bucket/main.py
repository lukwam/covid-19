"""Cloud Function to move GitHub to Bucket."""

import google.auth

from github import Github
from google.cloud import storage
from google.cloud import secretmanager_v1


def _get_github_token():
    """Return the github token from secret manager."""
    _, project = google.auth.default()
    client = secretmanager_v1.SecretManagerServiceClient()
    name = client.secret_version_path(project, 'github-token', 'latest')
    response = client.access_secret_version(name)
    print(response)
    return response


def github_to_bucket(request):
    """Move GitHub CSV to Bucket."""
    token = _get_github_token()
    g = Github(token)
    print(g)

