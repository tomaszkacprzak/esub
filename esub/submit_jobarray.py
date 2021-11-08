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
parser.add_argument('--function', type=str, required=True)
parser.add_argument('--executable', type=str, required=True)
parser.add_argument('--tasks', type=str, required=True)
parser.add_argument('--n_cores', type=int, required=True)
parser.add_argument('--system', type=str, required=True)

args, function_args = parser.parse_known_args()

# get rank of the processor
if args.system == 'bsub':
    rank = int(os.environ['LSB_JOBINDEX'])
    rank -= 1
elif args.system == 'slurm':
    rank = int(os.environ['SLURM_ARRAY_TASK_ID'])


# Import the executable
executable = utils.import_executable(args.executable)

LOGGER.info('Running the function {} specified in executable'.format(args.function))

# Change to local scratch if set
utils.cd_local_scratch()

# getting index list based on jobid
indices = utils.get_indices_splitted(args.tasks, args.n_cores, rank)

if args.function == 'main':

    LOGGER.info('Running {}, {} task(s), first: {}, last: {}'.format(args.function, len(indices), indices[0], indices[-1]))
    for index in getattr(executable, args.function)(indices, function_args):
        LOGGER.info('##################### Finished Task {} #####################'.format(index))

elif args.function == 'missing':

    indices_missing = getattr(executable, args.function)(indices, function_args)
    filepath_indices_rerun = utils.get_filepath_indices_rerun(args)
    utils.write_indices_yaml(filepath_indices_rerun, indices_missing)

else:
    LOGGER.info('Running {}, {} task(s), first: {}, last: {}'.format(args.function, len(indices), indices[0], indices[-1]))
    getattr(executable, args.function)(indices, function_args)



