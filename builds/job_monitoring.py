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


def monitor_gce_jobs(phase, sleep, release='release_v' + __version__, log='pkt_builder_logs.log'):
    """Monitors PheKnowLator build jobs for Phase 2 of the build process. Method monitors jobs by querying the
    current_build directory of the current release in the dedicated project Google Cloud Storage Bucket.

    Args:
        phase: An integer representing the build phase (i.e. 1 for "Phases1-2" or 3 for "Phase 3").
        sleep: An integer containing the number of seconds to wait between job status checks.
        release: A string containing the project release version.
        log: A string containing the name of the log file to monitor.

    Returns:
        status: A string containing the job's exit status.
    """

    gcs_current_build_log = 'https://storage.googleapis.com/pheknowlator/{}/current_build/{}'.format(release, log)
    if phase == 1: quit_status, log_name = 'COMPLETED BUILD PHASES 1-2', 'pkt_builder_phases12_log.log'
    else: quit_status, log_name = 'COMPLETED BUILD PHASE 3', 'pkt_builder_phase3_log.log'

    # query job status
    log_content, start_time, status = None, datetime.now(), 'RUNNING'
    while status == 'RUNNING':
        time.sleep(int(sleep))
        current_time = str(datetime.now().strftime('%b %d %Y %I:%M%p'))
        data_downloader(gcs_current_build_log, '.' + '/')  # download log and retrieve program state
        try:
            log_content = [json.loads(x) for x in open(log)]
            messages = ' '.join([x['message'] for x in log_content])
            if len([x for x in log_content if x['levelname'] == 'ERROR']) > 0: status = 'FAILED'
            elif quit_status in messages: status = 'COMPLETED'
            else: status = 'RUNNING'
        except JSONDecodeError: status, log_content = 'RUNNING', ['QUEUED: Program has not yet Started']
        elapsed_minutes = round((datetime.now() - start_time).total_seconds() / 60, 3)
        print('\n\nJob Status: {} @ {} -- Elapsed Time (min): {}\n'.format(status, current_time, elapsed_minutes))

        # print log to console -- printing this to update logs via GitHub Actions console
        for event in log_content:
            print(event)

    return status


@click.command()
@click.option('--instance_type', prompt='Indicate GCE Instance Type (e.g. "reg", "ai").')
@click.option('--phase', prompt='Provide an Integer Representing the Build Phase (i.e. "1" or "3").')
@click.option('--project', prompt='Provide the Google Project Identifier')
@click.option('--job', prompt='Provide the Job Name')
@click.option('--sleep', prompt='Provide the Time in Seconds to Sleep')
def main(instance_type, phase, project, job, sleep):

    start_time = datetime.now()

    # identify build phase and activate job monitoring
    if instance_type == "ai": state = monitor_ai_platform_jobs(project=project, job=job, sleep=sleep)
    else: state = monitor_gce_jobs(phase=int(phase), sleep=sleep)

    # print job run information
    current_time = str(datetime.now().strftime('%b %d %Y %I:%M%p'))
    elapsed_minutes = round((datetime.now() - start_time).total_seconds() / 60, 3)
    print('\nJOB STATUS: {}! {} -- Total Elapsed Time (min): {}'.format(state, current_time, elapsed_minutes))


if __name__ == '__main__':
    main()
