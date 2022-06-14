# Copyright (C) 2019 ETH Zurich,
# Institute for Particle Physics and Astrophysics
# Author: Joerg Herbel

import os
import argparse

from esub import utils

LOGGER = utils.get_logger(__file__)


def main(dirpath_logs):

    # Get all log files in directory
    filenames_logs = filter(lambda fn: not fn.startswith('.') and
                            os.path.splitext(fn)[1] == '.log',
                            os.listdir(dirpath_logs))
    filenames_logs = sorted(filenames_logs)

    # Check
    unfinished_logs = []

    for filename in filenames_logs:

        # read log file
        with open(os.path.join(dirpath_logs, filename), mode='r') as f:
            log = f.read()

        # check which functions were submitted
        main_run = 'Job id main' in log
        rerun_missing = 'Job id rerun_missing' in log
        merge_run = 'Job id merge' in log
        watchdog_run = 'Job id watchdog' in log

        # check if main job only and MPI mode
        if main_run and not (rerun_missing | merge_run | watchdog_run):
            if 'Finished running main' in log:
                continue

        # check jobarray case
        expected_content = []
        if main_run or rerun_missing:
            expected_content.append('All indices finished')
        if merge_run:
            expected_content.append('Finished running merge')
        if watchdog_run:
            expected_content.append('Finished running watchdog')

        for exp_cont in expected_content:
            if exp_cont not in log:
                unfinished_logs.append(filename)
                break

    # Report
    if len(unfinished_logs) == 0:
        LOGGER.info('No unfinished jobs found')
    else:
        LOGGER.info('Logs containing unfinished jobs:\n{}'.format(
            '\n'.join(unfinished_logs)))


if __name__ == '__main__':
    description = "Send a command to all jobs logged in " \
                  "esub logfiles in a given directory"
    parser = argparse.ArgumentParser(description=description, add_help=True)
    parser.add_argument('--dirpath_logs', type=str,
                        default='esub_logs',
                        help='Directory containing esub logs')
    args = parser.parse_args()
    main(args.dirpath_logs)
