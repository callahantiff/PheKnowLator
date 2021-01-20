#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import click
import datetime
import os
import re
import subprocess
import time

from datetime import datetime


@click.command()
@click.option('--project_name', prompt='The Google Project Identifier')
@click.option('--job_name', prompt='The Job Name')
def main(project_name, job_name):
    # tracks job status (source: https://cloud.google.com/ai-platform/training/docs/monitor-training)

    # get python representation of the REST API and specify project information
    job_id = 'projects/{}/jobs/{}'.format(project_name, job_name)

    # form the request and listen until completion
    start_time, state = datetime.now(), 'QUEUED'
    while state in ['RUNNING', 'QUEUED']:
        time.sleep(60)
        current_time = str(datetime.now().strftime('%b %d %Y %I:%M%p'))
        output = subprocess.check_output("gcloud ai-platform jobs describe {}".format(job_id), shell=True)
        state = re.findall(r'state.*(?=trainingInput)', str(output))[0].strip('state: ').strip('\\n')
        elapsed_minutes = round((datetime.now() - start_time).total_seconds()/60, 3)
        print('Job Status: {} @ {} -- Elapsed Time (min): {}'.format(state, current_time, elapsed_minutes))

    # print process completion information
    current_time = str(datetime.now().strftime('%b %d %Y %I:%M%p'))
    elapsed_minutes = round((datetime.now() - start_time).total_seconds() / 60, 3)
    print('\nJOB COMPLETE! {} -- Total Elapsed Time (min): {}'.format(current_time, elapsed_minutes))


if __name__ == '__main__':
    main()
