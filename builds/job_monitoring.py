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
        time.sleep(sleep)
        current_time = str(datetime.now().strftime('%b %d %Y %I:%M%p'))
        state = re.findall(state_pattern, str(subprocess.check_output(query, shell=True)))[0]
        elapsed_minutes = round((datetime.now() - start_time).total_seconds() / 60, 3)
        print('Job Status: {} @ {} -- Elapsed Time (min): {}'.format(state, current_time, elapsed_minutes))

    # print process completion information
    final_state = re.findall(state_pattern, str(subprocess.check_output(query, shell=True)))[0]
    if state == 'FAILED': raise Exception('Job Failed: {}'.format(final_state))
    else: state = 'COMPLETED'

    return state


def monitor_gce_jobs(phase, sleep, gcs_log_location):
    """Monitors PheKnowLator build jobs for Phase 2 of the build process. Method monitors jobs by querying the
    current_build directory of the current release in the dedicated project Google Cloud Storage Bucket.

    Args:
        phase: An integer representing the build phase (i.e. 1 for "Phases1-2" or 3 for "Phase 3").
        sleep: An integer containing the number of seconds to wait between job status checks.
        gcs_log_location: A string containing a Google Cloud Storage file path and the name of the log file to monitor.

    Returns:
        status: A string containing the job's exit status.
    """

    quit_status = 'EXIT BUILD PHASES 1-2' if phase == 1 else 'EXIT BUILD PHASE 3'

    # query job status
    log_content, start_time, status = None, datetime.now(), 'RUNNING'
    while status == 'RUNNING':
        time.sleep(sleep)
        current_time = str(datetime.now().strftime('%b %d %Y %I:%M%p'))
        data_downloader(gcs_log_location, '.' + '/')  # download log and retrieve program status
        try:
            log_content = [json.loads(x) for x in open(gcs_log_location.split('/')[-1])]
            messages = ' '.join([x['message'] for x in log_content])
            if len([x for x in log_content if x['levelname'] == 'ERROR']) > 0: status = 'FAILED'
            elif quit_status == messages: status = 'COMPLETED'
            else: status = 'RUNNING'
        except JSONDecodeError: status, log_content = 'RUNNING', ['QUEUED: Instance is Queued or being Prepared']
        elapsed_minutes = round((datetime.now() - start_time).total_seconds() / 60, 3)
        print('\n\nJob Status: {} @ {} -- Elapsed Time (min): {}\n'.format(status, current_time, elapsed_minutes))

        # print log to console -- printing this to update logs via GitHub Actions console
        for event in log_content:
            print(event)

    return status


@click.command()
@click.option('--gce_type', prompt='Indicate GCE Instance Type (e.g. "reg", "ai").', default='reg')
@click.option('--phase', prompt='Integer Representing the Build Phase (i.e. "1" or "3").', type=int)
@click.option('--sleep', prompt='Time in Seconds to Sleep when Monitoring', default=60, type=int)
@click.option('--gcs_dir', prompt="Write directory within GCS current_build directory.", required=False, default='')
@click.option('--project', prompt='Google Project Identifier', required=False, default='')
@click.option('--job', prompt='Google Job Name', required=False, default='')
def main(gce_type, phase, sleep, gcs_dir, project, job):

    start_time = datetime.now()

    # set log name -- used for monitoring "reg" or regular GCE instances and gcs log location
    if phase == 1:
        gcs_url_string = 'https://storage.googleapis.com/pheknowlator/current_build/{}'
        gcs_log_location = gcs_url_string.format('pkt_builder_phases12_log.log')
    else:
        gcs_url_string = 'https://storage.googleapis.com/pheknowlator/current_build/knowledge_graphs/{}/{}'
        gcs_log_location = gcs_url_string.format(gcs_dir, 'pkt_builder_phase3_log.log')

    # identify build phase and activate job monitoring
    if gce_type == "ai": state = monitor_ai_platform_jobs(project=project, job=job, sleep=sleep)
    else: state = monitor_gce_jobs(phase=phase, sleep=sleep, gcs_log_location=gcs_log_location)

    # print job run information
    current_time = str(datetime.now().strftime('%b %d %Y %I:%M%p'))
    elapsed_minutes = round((datetime.now() - start_time).total_seconds() / 60, 3)
    print('\nJOB STATUS: {}! {} -- Total Elapsed Time (min): {}'.format(state, current_time, elapsed_minutes))


if __name__ == '__main__':
    main()
