#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import click
import datetime
import os
import time

from datetime import datetime
from dateutil import parser
from googleapiclient import discovery
from googleapiclient import errors
from oauth2client.client import GoogleCredentials


@click.command()
@click.option('--project_name', prompt='The Google Project Identifier')
@click.option('--job_name', prompt='The Job Name')
@click.option('--credentials', prompt='A Github secret containing the Google Credential Information')
def main(project_name, job_name, credential):
    # tracks job status (source: https://cloud.google.com/ai-platform/training/docs/monitor-training)

    # set environment variable
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials

    # get python representation of the REST API and specify project information
    cloud_ml = discovery.build('ml', 'v1')
    job_id = 'projects/{}/jobs/{}'.format(project_name, job_name)

    # form the request and listen until completion
    start_time, state = datetime.now(), 'RUNNING'
    while state == 'RUNNING':
        time.sleep(60)
        request = cloud_ml.projects().jobs().get(name=job_id).execute()
        state, current_time = request['state'], str(datetime.now().strftime('%b %d %Y %I:%M%p'))
        elapsed_minutes = round((datetime.now() - start_time).total_seconds()/60, 3)
        print('Job Status: {} @ {} -- Elapsed Time (min): {}'.format(state, current_time, elapsed_minutes))

    # print process completion information
    current_time = str(datetime.now().strftime('%b %d %Y %I:%M%p'))
    elapsed_minutes = round((datetime.now() - start_time).total_seconds() / 60, 3)
    print('\nJOB COMPLETE! {} -- Total Elapsed Time (min): {}'.format(current_time, elapsed_minutes))


if __name__ == '__main__':
    main()
