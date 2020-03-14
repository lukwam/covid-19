"""Cloud Function to move GitHub to Bucket."""

from github import Github
from google.cloud import storage


def github_to_bucket(request):
    """Move GitHub CSV to Bucket."""
