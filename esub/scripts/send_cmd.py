# Copyright (C) 2019 ETH Zurich,
# Institute for Particle Physics and Astrophysics
# Author: Joerg Herbel

import os
import shlex
import subprocess
import argparse

from esub import utils

LOGGER = utils.get_logger(__file__)


def get_job_ids(path_log):

    # Read file
    with open(path_log, 'r') as f:
        lines = f.readlines()

    # Find job ids
    job_ids = []

    for line in lines:
        if 'Job id' in line:

            job_id = ''
            line = line.strip()
            i = len(line) - 1

            while line[i].isdigit():
                job_id = line[i] + job_id
                i -= 1

            job_ids.append(job_id)

    return job_ids


def send_to_job(cmd, job_id):
    full_cmd = '{} {}'.format(cmd, job_id)
    subprocess.call(shlex.split(full_cmd))


def main(dirpath_logs, cmd, log_filter=None):

    # Get all log files in directory
    filenames_logs = filter(lambda fn: not fn.startswith('.') and
                            os.path.splitext(fn)[1] == '.log',
                            os.listdir(dirpath_logs))

    # Filter files
    if log_filter is not None:
        filenames_logs = filter(lambda fn: log_filter in fn, filenames_logs)

    # Extract job ids from files
    job_ids = []
    for filename in filenames_logs:
        job_ids += get_job_ids(os.path.join(dirpath_logs, filename))

    # Modify jobs
    for job_id in job_ids:
        LOGGER.info('Sending cmd {} to job {}'.format(cmd, job_id))
        send_to_job(cmd, job_id)


if __name__ == '__main__':
    description = "Send a command to all jobs logged in esub " \
                  "logfiles in a given directory"
    parser = argparse.ArgumentParser(description=description, add_help=True)
    parser.add_argument('--dirpath_logs', type=str,
                        default='esub_logs',
                        help='Directory containing esub logs')
    parser.add_argument('--cmd', type=str, required=True,
                        help='Command to send to jobs')
    parser.add_argument('--log_filter', type=str,
                        help='String that must be contained in log filename '
                             'to receive cmd, default: all log files')
    args = parser.parse_args()

    main(args.dirpath_logs, args.cmd, log_filter=args.log_filter)
