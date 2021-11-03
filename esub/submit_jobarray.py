#! /usr/bin/env python

# Copyright (C) 2019 ETH Zurich,
# Institute for Particle Physics and Astrophysics
# Author: Dominik Zuercher

# System imports
from __future__ import (print_function, division, absolute_import,
                        unicode_literals)

# package imports
import os
import argparse
import time
from esub import esub, utils

LOGGER = utils.get_logger(__file__)


# parse all the submitter arguments
parser = argparse.ArgumentParser()
parser.add_argument('--job_name', type=str, required=True)
parser.add_argument('--source_file', type=str, required=True)
parser.add_argument('--main_memory', type=float, required=True)
parser.add_argument('--main_time', type=float, required=True)
parser.add_argument('--main_scratch', type=float, required=True)
parser.add_argument('--function', type=str, required=True)
parser.add_argument('--executable', type=str, required=True)
parser.add_argument('--tasks', type=str, required=True)
parser.add_argument('--n_cores', type=int, required=True)
parser.add_argument('--log_dir', type=str, required=True)
parser.add_argument('--system', type=str, required=True)
parser.add_argument('--main_name', type=str, required=True)

args, function_args = parser.parse_known_args()
function = args.function
source_file = args.source_file
job_name = args.job_name
log_dir = args.log_dir
exe = args.executable
tasks = args.tasks
n_cores = args.n_cores
main_memory = args.main_memory
main_time = args.main_time
main_scratch = args.main_scratch
system = args.system
main_name = args.main_name

# get path of log file and of file containing finished indices
path_log = utils.get_path_log(log_dir, job_name)
path_finished = utils.get_path_finished_indices(log_dir, job_name)

# get rank of the processor
if system == 'lsf':
    rank = int(os.environ['LSB_JOBINDEX'])
    rank -= 1
elif system == 'slurm':
    rank = int(os.environ['SLURM_ARRAY_TASK_ID'])
else:
    raise Exception(f'system {system} not supported')


# Import the executable
executable = utils.import_executable(exe)

if function == 'main':
    LOGGER.info('Running the function {} specified in executable'.format(main_name))
else:
    LOGGER.info('Running the function {} specified in executable'.format(function))

if function == 'rerun_missing':
    LOGGER.info('Checking if all main jobs terminated correctly...')
    indices_all = utils.get_indices(tasks)

    indices_missing = utils.check_indices(
        indices_all, path_finished, executable, function_args, LOGGER)

    utils.write_to_log(
        path_log, 'Found {} missing indices'.format(len(indices_missing)))

    if len(indices_missing) == 0:
        LOGGER.info('Nothing to resubmit. All jobs ended.')
    else:

        if len(indices_missing) > 1:
            tasks = ','.join(map(str, indices_missing[:-1]))
            n_cores = len(indices_missing) - 1
            LOGGER.info(
                'Re-Submitting tasks {} to {} cores'.format(tasks, n_cores))
            jobid = esub.submit_job(tasks=tasks, mode='jobarray',
                                    exe=args.executable, log_dir=log_dir,
                                    function_args=function_args,
                                    function='main', source_file=source_file,
                                    n_cores=n_cores, job_name=job_name,
                                    main_memory=main_memory,
                                    main_time=main_time,
                                    main_scratch=main_scratch, dependency='',
                                    system=system, main_name=main_name)

            utils.write_to_log(
                path_log, 'Job id rerun_missing extended: {}'.format(jobid))

        # Change to local scratch if set; this has to be done after submission,
        # s.t. that the pwd at submission time is
        # the original directory where the submission starts
        utils.cd_local_scratch()

        # run last job locally to not waste any resources
        index = indices_missing[-1]
        LOGGER.info('##################### Starting Task {} #####################'.format(index))
        for index in getattr(executable, main_name)([index], function_args):
            utils.write_index(index, path_finished)
        LOGGER.info('##################### Finished Task {} #####################'.format(index))

        if len(indices_missing) == 1:
            utils.write_to_log(path_log, 'First index is done')

    # wait until all jobs are done
    while True:
        indices_missing = utils.check_indices(
            indices_all, path_finished, executable, function_args, LOGGER)

        if len(indices_missing) == 0:
            LOGGER.info('All indices finished')
            utils.write_to_log(path_log, 'All indices finished')
            break

        time.sleep(60)

else:

    # Change to local scratch if set
    utils.cd_local_scratch()

    # getting index list based on jobid
    indices = utils.get_indices_splitted(tasks, n_cores, rank)

    if function == 'main':

        LOGGER.info('Running on tasks: {}'.format(indices))

        is_first = rank == 0

        for index in getattr(executable, main_name)(indices, function_args):
            utils.write_index(index, path_finished)
            LOGGER.info(
                '##################### Finished Task {} #####################'.format(index))

            if is_first:
                utils.write_to_log(path_log, 'First index is done')
                is_first = False

    elif function == 'missing':

        indices_missing = getattr(executable, function)(indices, function_args)
        filepath_indices_rerun = utils.get_filepath_indices_rerun(args)
        utils.write_indices_yaml(filepath_indices_rerun, indices_missing)
        utils.write_to_log(path_log, 'Finished running {}'.format(function))

    else:
        utils.write_to_log(path_log, 'Running {}'.format(function))
        LOGGER.info('Running {}, {} task(s), first: {}, last: {}'.format(function, len(indices), indices[0], indices[-1]))
        getattr(executable, function)(indices, function_args)
        utils.write_to_log(path_log, 'Finished running {}'.format(function))



