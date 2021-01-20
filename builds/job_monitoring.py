#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import click
import os
import re
import subprocess
import time

from datetime import datetime


@click.command()
@click.option('--project', prompt='The Google Project Identifier')
@click.option('--job', prompt='The Job Name')
@click.option('--sleep', prompt='The Time in Seconds to Sleep')
def main(project, job, sleep):
    # monitors google ml cloud job status (source: https://cloud.google.com/ai-platform/training/docs/monitor-training)

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
        elapsed_minutes = round((datetime.now() - start_time).total_seconds()/60, 3)
        print('Job Status: {} @ {} -- Elapsed Time (min): {}'.format(state, current_time, elapsed_minutes))

    # print process completion information
    final_state = re.findall(state_pattern, str(subprocess.check_output(query, shell=True)))[0]
    if state == 'FAILED':
        raise Exception('Job Failed: {}'.format(final_state))
    else:
        current_time = str(datetime.now().strftime('%b %d %Y %I:%M%p'))
        elapsed_minutes = round((datetime.now() - start_time).total_seconds() / 60, 3)
        print('\nJOB COMPLETE! {} -- Total Elapsed Time (min): {}'.format(current_time, elapsed_minutes))


if __name__ == '__main__':
    main()
