#! /usr/bin/env python

# Copyright (C) 2019 ETH Zurich, Institute for Particle Physics and Astrophysics
# Author: Dominik Zuercher

# System imports
from __future__ import (print_function, division, absolute_import,
                        unicode_literals)

# package imports
import sys
import argparse
import subprocess
import shlex
import yaml

from esub import utils, esub

LOGGER = utils.get_logger(__file__)

def starter_message():
    print()
    print(' ______   ______   __    ______   ______  ')
    print('| _____| |   _  | |  |  |   _  | | _____| ')
    print('| |___   |  |_| | |  |  |  |_| | | |___   ')
    print('|  ___|  | _____| |  |  | _____| |  ___|  ')
    print('| |____  | |      |  |  | |      | |____  ')
    print('|______| |_|      |__|  |_|      |______| ')
    print()


def format_loop_cmd(command, index):
    """
    Inserts a loop index into a command if specified by eg. "{}"
    inside the command. Then evaluates the command
    as an f-string to resolve possible (e.g. mathematical) expressions.

    :param command: command string
    :param index: loop index
    :return: command with loop index inserted at all places specified by
             curly brackets and optional format
             specifications and afterwards as f-string evaluated
    """

    # format
    index_repeated = []

    while True:
        try:
            # if this fails, there are more places where the index
            # should be inserted than we currently have items in
            # the list
            command_formatted = command.format(*index_repeated)
            break
        except IndexError:
            index_repeated.append(index)

    # evaluate as f-string
    command_formatted = eval('f"{}"'.format(command_formatted))

    return command_formatted


def represents_int(s):
    """
    Checks if string can be converted to integer

    :param s: string to be checked
    :return: True if s can be converted to an integer and False otherwise
    """
    try:
        int(s)
        return True
    except ValueError:
        return False


def make_submit_command(base_cmd, name, deps, job_dict, parameters):
    """
    Adds the dependencies to the base command given in the pipeline
    Also tries to replace variables indicated by [] brackets.

    :param base_cmd: The command string given in pipeline file
    :param name: The job name
    :param deps: The dependencies of the job
    :param job_dict: The jobs dictionary
    :param parameters: A dictionary holding global variables.
                       Function attempts to replace them in the command string
    :return: Complete submission string
    """

    dep_string = ''
    for dep in deps:
        if represents_int(dep):
            job_ids = [dep]
        elif dep in job_dict.keys():
            job_ids = job_dict[dep]
        else:
            LOGGER.info(
                "Did not find a job named {}. \
                 Ignoring dependencies on it".format(dep))
            continue
        for job_id in job_ids:
            dep_string += 'ended({}) && '.format(int(job_id))
    dep_string = dep_string[:-4]
    dep_string = '"' + dep_string + '"'

    cmd = base_cmd + ' --job_name={}'.format(name) 
    if dep_string!='""':
        cmd += ' --dependency={}'.format(dep_string)

    # Attempt variable replacement
    while True:
        rep = cmd.find('$')
        if rep == -1:
            break
        start = cmd[rep:].index('[') + rep
        stop = cmd[rep:].index(']') + rep
        rep_key = cmd[start + 1: stop]
        cmd = cmd[:rep] + str(parameters[rep_key]) + cmd[stop + 1:]
    return cmd


def submit(cmd, assert_ids=True, verb=False):
    """
    Runs the command and receives its job index
    which gets added to the dependency list.

    :param cmd: Command string to submit
    :param assert_ids: whether to check that job ids were successfully obtained
    :return: The ids of the just submitted jobs
    """

    LOGGER.info('Running {}'.format(cmd))

    stdout_lines = []
    stderr_lines = []
    with subprocess.Popen(shlex.split(cmd),
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          bufsize=1,
                          universal_newlines=True) as proc:

        # read stdout
        for line in proc.stdout:
            stdout_lines.append(line)

            if esub.TIMEOUT_MESSAGE in line:
                LOGGER.info(esub.TIMEOUT_MESSAGE)

        # read stderr
        stderr = '\n'.join(proc.stderr)
        for line in proc.stderr:
            stderr_lines.append(line)

    if verb:
        LOGGER.info('esub stdout:')
        print(' '.join(stdout_lines))
        LOGGER.info('esub stderr:')
        print(' '.join(stderr_lines))


    # raise an exception in case the process did not terminate successfully
    if proc.returncode != 0:
        raise RuntimeError('Running the command \"{}\" failed with\
                            exit code {}. Error: \n{}'. format(cmd,
                                                               proc.returncode,
                                                               stderr))

    # get ids of newly submitted jobs
    last_line = stdout_lines[-1]

    ids = []
    for string in last_line.split(' '):
        try:
            int(string)
            ids.append(string)
        except ValueError:
            pass

    if assert_ids:
        assert len(
            ids) > 0, 'Something went wrong, did not manage\
                       to get any job ids from stdout'

    print()

    return ids


def get_parameters(step):
    """
    Extracts and stores global variables from the\
    parameter object in epipe file

    :param step: epipe object instance
    :return : Dictionary holding the global variables
    """
    param_list = step['parameters']
    parameters = {}
    for element in param_list:
        key = list(element.keys())[0]
        parameters[key] = element[key]

    parameter_dict = {}
    for par_key in parameters.keys():
        parameter_dict[par_key] = parameters[par_key]
    return parameter_dict


def process_item(step, job_dict, index=-1, loop_dependence=None,
                 parameters=None, assert_ids=True, verb=False, jc_flow=False):
    """
    Processes one of the epipe items in the pipeline

    :param step: The job item
    :param job_dict: The previous job id dictionary
    :param index: Loop index if within a loop
    :param loop_dependence: The overall dependencies of the
                            loop structure if within a loop
    :param parameters: A dictionary holding global variables. Function
                       attempts to replace them in the command string
    :param assert_ids: whether to check that job ids were successfully obtained
    :return: An updated dictionary containing the already
             submitted jobs and the newly submitted job
    """

    if parameters is None:
        parameters = dict()

    deps = []
    in_loop = index >= 0

    # direct dependencies of the item itself
    if 'dep' in step.keys():
        step['dep'] = str(step['dep'])
        dependencies = step['dep'].replace(' ', '').split(',')
        for dep in dependencies:
            deps.append(dep)

        # if we are inside a loop, then format the
        # loop-internal dependencies as necessary
        if in_loop:
            for ii, dep in enumerate(deps):
                if not represents_int(dep):
                    deps[ii] = '{}__{}'.format(dep, index)

    # add the global dependencies of the loop
    if loop_dependence is not None:
        for lp in loop_dependence:
            deps.append(lp)

    # get the job name and the job command
    job_name = step['name']
    base_cmd = step['cmd']
    if in_loop:
        job_name = '{}__{}'.format(job_name, index)
        base_cmd = format_loop_cmd(base_cmd, index)
        base_cmd += ' -jc'
    else:
        base_cmd += ' -jc'
    # submit the job
    LOGGER.info('Submitting job {}'.format(job_name))
    cmd = make_submit_command(base_cmd, job_name, deps, job_dict, parameters)
    new_ids = submit(cmd, assert_ids=assert_ids, verb=verb)
    job_dict[job_name] = new_ids


def main(args=None):

    """
    epipe main function. Accepts a epipe yaml
    configuration file and runs the pipeline.

    :param args: Command line arguments
    """

    if args is None:
        args = sys.argv[1:]

    description = "This is epipe a tool to easily submit\
                   pipelines to a clusters queing system"
    parser = argparse.ArgumentParser(description=description, add_help=True)
    # parse all the submitter arguments
    parser.add_argument('pipeline', type=str, action='store',
                        help='Path to the pipeline file. Format should be as\
                              specified in the epipe example. Check\
                              Documentations.')
    parser.add_argument('--ignore_jobid_errors', action='store_true',
                        help='Switches off check whether ids of submitted\
                              jobs were successfully obtained, useful for\
                              testing.')
    parser.add_argument('-v', '--verb', action='store', type=bool, default=True,
                        help='If to show esub output.')
    parser.add_argument('-jc', '--jobchainer_flow', action='store_true',
                    help='add --jobchainer_flow to all jobs.')


    args = parser.parse_args(args)
    pipeline = args.pipeline
    assert_ids = not args.ignore_jobid_errors
    verb=args.verb
    jc_flow=args.jobchainer_flow

    # read pipeline file
    with open(pipeline, 'r') as f:
        pipeline = yaml.load(f, Loader=yaml.FullLoader)

    starter_message()

    # parse all the jobs and their dependencies from the pipeline string
    job_dict = {}

    LOGGER.info('Starting submission')
    print()

    parameters = None

    for step in pipeline:
        if 'parameters' in step.keys():
            LOGGER.info("Found parameter object")
            parameters = get_parameters(step)
            LOGGER.info("Have set global parameters as {}".format(parameters))
            continue

        if 'loop' in step.keys():

            loop_indices = list(step['loop'])
            deps = []
            # get the names of all jobs that have been
            # submitted before the loop
            submitted_jobs = set(job_dict.keys())

            if 'dep' in step.keys():
                step['dep'] = str(step['dep'])
                dependencies = step['dep'].replace(' ', '').split(',')
                for dep in dependencies:
                    deps.append(dep)

            for index in range(loop_indices[0], loop_indices[1]):
                for item in step['items']:
                    process_item(item, job_dict, index=index,
                                 loop_dependence=deps, parameters=parameters,
                                 assert_ids=assert_ids, verb=verb, jc_flow=jc_flow)
            # find the names of the newly submitted jobs
            loop_jobs = set(job_dict.keys()) - submitted_jobs

            # add the loop as a whole to the job dictionary
            job_dict[step['name']] = []
            for loop_job in loop_jobs:
                job_dict[step['name']] += job_dict[loop_job]

        else:

            if 'assert_ids' in step.keys():
                assert_ids = step['assert_ids']
            else:
                assert_ids = not args.ignore_jobid_errors
       
            process_item(step, job_dict, parameters=parameters,
                         assert_ids=assert_ids, verb=verb, jc_flow=jc_flow)

    LOGGER.info('Submission finished')


if __name__ == '__main__':
    main()
