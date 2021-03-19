#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import glob
import ray
import threading
import time

from google.cloud import storage  # type: ignore

from builds.build_utilities import uploads_data_to_gcs_bucket  # type: ignore


@ray.remote
class PKTLogUploader(object):
    """Implements a background task that uploads logs from a directory to a Google Cloud Storage Bucket while another
    method, external to the class in run.

    SOURCE: https://github.com/ray-project/ray/issues/854#issuecomment-499056709

        Attributes:
            bucket_name: A string specifying the name of a Google Cloud Storage bucket.
            gcs_bucket_location: A string specifying the location of the original_data directory for a specific build.
            log_directory: A local directory where preprocessed data is stored.
            sleep_interval: An integer specifying how often logs should be pushed to the Google Cloud Storage Bucket.
    """

    def __init__(self, bucket_name, gcs_bucket_location, log_directory, sleep_interval):
        self.bucket: str = storage.Client().get_bucket(bucket_name)
        self.gcs_bucket_location: str = gcs_bucket_location
        self.log_directory: str = log_directory
        self.sleep: int = sleep_interval
        self.kill_time: int = 172800  # 48 hours

        # START BACKGROUND PROCESS
        self._thread: threading.Thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        """Method uploads any log files found in temp_directory from a local directory to a specific directory in a
        Google Cloud Storage Bucket every "n" minutes as specified by the input interval variable. This method runs
        the program it is called with finishes. There is also a back-up timer that will kill the program

        Args:
            bucket: A storage Bucket object specifying a Google Cloud Storage bucket.
            original_data: A string specifying the location of the original_data directory for a specific build.
            temp_directory: A local directory where preprocessed data is stored.
            interval: An integer specifying how often the data should be pushed up to the Google Cloud Storage Bucket.

        Returns:
            None.
        """

        # grep for log files in the log_directory
        log_file = glob.glob(self.log_directory + '/*.log')[0].split('/')[-1]
        runtime = 0

        while runtime < self.kill_time:
            uploads_data_to_gcs_bucket(self.bucket, self.gcs_bucket_location, self.log_directory, log_file)
            time.sleep(self.sleep)
            runtime += self.sleep

        return None
