#!/bin/bash

export PATH=/opt/google-cloud-sdk/bin:$PATH

PROJECT='broad-covid-19'
IMAGE="gcr.io/${PROJECT}/covid-19:latest"

# pull the newest image
docker pull ${IMAGE}

docker run -it --rm \
  -e GOOGLE_APPLICATION_CREDENTIALS=/usr/src/etc/service_account.json \
  -v `pwd`:/usr/src \
  -w /usr/src \
  ${IMAGE} \
  python main.py
