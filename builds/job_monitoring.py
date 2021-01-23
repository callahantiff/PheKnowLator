#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import click
import json
import os
import re
import subprocess
import time

from datetime import datetime
from json import JSONDecodeError

from pkt_kg.__version__ import __version__
from pkt_kg.utils import data_downloader


def monitor_ai_platform_jobs(project, job, sleep):
    """Monitors PheKnowLator build jobs for Phases 1-2 of the build process. Method monitors jobs by querying GCloud
    for the current status of a job running on the AI Platform.

    Args:
        project: A string containing the name of the Google project.
        job: A string containing the name of the job submitted to the Google AI Platform.
        sleep: An integer containing the number of seconds to wait between job status checks.

    Returns:
        state: A string containing the job's exit status.
    """
    # create project identifier
    job_id = 'projects/{}/jobs/{}'.format(project, job)

    # set pattern to extract job state
    state_pattern, query = r'(?<=state:\s).*(?=\\ntrainingInput)', "gcloud ai-platform jobs describe {}".format(job_id)

    # query job status
    start_time, state = datetime.now(), 'QUEUED'
    while state in ['RUNNING', 'QUEUED', 'PREPARING']:
        time.sleep(int(sleep))
        current_time = str(datetime.now().strftime('%b %d %Y %I:%M%p'))
        state = re.findall(state_pattern, str(subprocess.check_output(query, shell=True)))[0]
        elapsed_minutes = round((datetime.now() - start_time).total_seconds() / 60, 3)
        print('Job Status: {} @ {} -- Elapsed Time (min): {}'.format(state, current_time, elapsed_minutes))

    # print process completion information
    final_state = re.findall(state_pattern, str(subprocess.check_output(query, shell=True)))[0]
    if state == 'FAILED': raise Exception('Job Failed: {}'.format(final_state))
    else: state = 'COMPLETED'

    return state


def monitor_gce_jobs(sleep, release='release_v' + __version__, log='pkt_builder_logs.log'):
    """Monitors PheKnowLator build jobs for Phase 2 of the build process. Method monitors jobs by querying the
    current_build directory of the current release in the dedicated project Google Cloud Storage Bucket.

    Args:
        sleep: An integer containing the number of seconds to wait between job status checks.
        release: A string containing the project release version.
        log: A string containing the name of the log file to monitor.

    Returns:
        state: A string containing the job's exit status.
    """

    gcs_current_build_log = 'https://storage.googleapis.com/pheknowlator/{}/current_build/{}'.format(release, log)

    # query job status
    state_content, start_time, state = None, datetime.now(), 'RUNNING'
    while state == 'RUNNING':
        time.sleep(int(sleep))
        current_time = str(datetime.now().strftime('%b %d %Y %I:%M%p'))
        # get program state
        data_downloader(gcs_current_build_log, '.' + '/')
        try:
            state_content = [json.loads(x) for x in open(log)]
            messages = ' '.join([x['message'] for x in state_content])
            if len([x for x in state_content if x['levelname'] == 'ERROR']) > 0: state = 'FAILED'
            elif 'FINISHED PHEKNOWLATOR KNOWLEDGE GRAPH BUILD' in messages: state = 'COMPLETED'
            else: state = 'RUNNING'
        except JSONDecodeError: state, state_content = 'RUNNING', ['LOG FILE HAS NOT BEEN GENERATED YET']
        # get timestamp
        elapsed_minutes = round((datetime.now() - start_time).total_seconds() / 60, 3)
        print('\n\nJob Status: {} @ {} -- Elapsed Time (min): {}\n'.format(state, current_time, elapsed_minutes))

        # print log to console -- printing this to update logs via GitHub Actions console
        for res in state_content:
            print(res)

    return state


@click.command()
@click.option('--phase', prompt='An Integer containing the Build Phase (i.e. 1 or 3).')
@click.option('--project', prompt='The Google Project Identifier')
@click.option('--job', prompt='The Job Name')
@click.option('--sleep', prompt='The Time in Seconds to Sleep')
def main(phase, project, job, sleep):

    start_time = datetime.now()

    # identify build phase and activate job monitoring
    if int(phase) == 1: state = monitor_ai_platform_jobs(project, job, sleep)
    else: state = monitor_gce_jobs(sleep)

    # print job run information
    current_time = str(datetime.now().strftime('%b %d %Y %I:%M%p'))
    elapsed_minutes = round((datetime.now() - start_time).total_seconds() / 60, 3)
    print('\nJOB STATUS: {}! {} -- Total Elapsed Time (min): {}'.format(state, current_time, elapsed_minutes))


if __name__ == '__main__':
    main()
