#! /usr/bin/env python

# Copyright (C) 2019 ETH Zurich,
# Institute for Particle Physics and Astrophysics
# Author: Dominik Zuercher

# System imports
from __future__ import (print_function, division, absolute_import,
                        unicode_literals)

# package import
import argparse
import math
import os
import sys
import collections
import subprocess
import shlex
import time

from esub import utils

LOGGER = utils.get_logger(__file__)
TIMEOUT_MESSAGE = 'Maximum number of pending jobs reached, ' \
                  'will sleep for 30 minutes and retry'


def starter_message():

    msg = """
    ███████╗███████╗██╗   ██╗██████╗ 
    ██╔════╝██╔════╝██║   ██║██╔══██╗
    █████╗  ███████╗██║   ██║██████╔╝
    ██╔══╝  ╚════██║██║   ██║██╔══██╗
    ███████╗███████║╚██████╔╝██████╔╝
    ╚══════╝╚══════╝ ╚═════╝ ╚═════╝ 
    """
    print(msg)
                


def decimal_hours_to_str(dec_hours):
    """Transforms decimal hours into the hh:mm format

    :param dec_hours: decimal hours, float or int
    :return: string in the format hh:mm
    """

    full_hours = math.floor(dec_hours)
    minutes = math.ceil((dec_hours - full_hours) * 60)

    if minutes == 60:
        full_hours += 1
        minutes = 0

    if minutes < 10:
        time_str = '{}:0{}'.format(full_hours, minutes)
    else:
        time_str = '{}:{}'.format(full_hours, minutes)

    return time_str

def get_jobchainer_function_flow(args, executable):
    """
    Gets the configuration for jobchainer flow submission.
    :param args: Command line arguments are parsed
    :return: a list of configurations to run, the items of the list are
    list_functions[0]: commant to run, main, merge, missing, or something else
    list_functions[1]: start condition, empty (default), after or along the previous job
    list_functions[2]: number of cores to use for that command
    list_functions[3]: index of the rerun
    list_functions[4]: filename of the indices file
    """

    def add_reruns(n_rerun):

        import numpy as np
        n_cores_rerun = int(np.ceil(args.n_cores*0.1))
        for nr in range(1, n_rerun+1):
            list_functions['missing{:d}'.format(nr)] = {'fun': 'missing',  'dep': 'main{:d}'.format(nr-1), 'start': """numended({jid:s},*)""", 'ncor': 1, 'find': filepath_indices}
            filepath_indices_rerun = utils.get_filepath_indices_rerun(args)
            list_functions['main{:d}'.format(nr)] =   {'fun': 'main',     'dep': 'missing{:d}'.format(nr), 'start': """numended({jid:s},*)""", 'ncor': n_cores_rerun, 'find': filepath_indices_rerun}

    list_functions = collections.OrderedDict()

    filepath_indices = utils.get_filepath_indices(args)
    indices = utils.read_indices_yaml(filepath_indices)

    if hasattr(executable, 'preprocess'):
        main_dep = 'preprocess0'
        main_start = """numended({jid:s},*)"""
    else:
        main_dep = ''
        main_start = ''

    if args.merge_depenency_mode == 'after':
        merge_start = """numended({jid:s},*)"""
        if args.n_rerun_missing==0:
            merge_dep = 'main0'
        else:
            merge_dep = 'main{:d}'.format(args.n_rerun_missing)

    elif args.merge_depenency_mode == 'along':
        merge_start = """(numended({jid:s}, > 0) || numrun({jid:s}, > 0))"""
        merge_dep = 'main0'
    else:
        raise Exception('unknown args.merge_depenency_mode={}'.format(args.merge_depenency_mode))

    if args.function == 'all':

        if hasattr(executable, 'preprocess'):
            list_functions['preprocess0'] = {'fun': 'preprocess',  'dep': '', 'start': '', 'ncor': 1, 'find': filepath_indices}
        list_functions['main0']  = {'fun': 'main',   'dep': main_dep, 'start': main_start, 'ncor': args.n_cores, 'find': filepath_indices}
        add_reruns(args.n_rerun_missing)
        list_functions['merge0'] = {'fun': 'merge',  'dep': merge_dep, 'start': merge_start, 'ncor': 1, 'find': filepath_indices}

    else:

        for fun in args.function:

            if fun in ['missing', 'merge', 'preprocess']:
                nc = 1
            elif fun == 'main':
                nc = args.n_cores
            else:
                nc = args.n_cores

            list_functions[fun]  = {'fun': fun,   'dep': '', 'start': '', 'ncor': nc, 'find': filepath_indices}

    return list_functions




def make_resource_string(function, main_memory, main_time, main_scratch, main_nproc, preprocess_memory, preprocess_time, preprocess_scratch, preprocess_nproc, merge_memory, merge_time, merge_scratch, merge_nproc, system):
    """
    Creates the part of the submission string which handles
    the allocation of ressources

    :param function: The name of the function defined
                     in the executable that will be submitted
    :param main_memory: Memory per core to allocate for the main job
    :param main_time: The Wall time requested for the main job
    :param main_scratch: Scratch per core to allocate for the main job
    :param preprocess_memory: Memory per core to allocate for the preprocess job
    :param preprocess_time: The Wall time requested for the preprocess job
    :param preprocess_scratch: Scratch to allocate for the preprocess job
    :param merge_memory: Memory per core to allocate for the merge job
    :param merge_time: The Wall time requested for the merge job
    :param merge_scratch: Scratch to allocate for the merge job
    :param system: The type of the queing system of the cluster
    :return: A string that is part of the submission string.
    """

    if function == 'main':
        mem = main_memory
        time = main_time
        scratch = main_scratch
        nproc = main_nproc
    elif function == 'preprocess':
        mem = preprocess_memory
        time = preprocess_time
        scratch = preprocess_scratch
        nproc = preprocess_nproc
    elif function == 'merge':
        mem = merge_memory
        time = merge_time
        scratch = merge_scratch
        nproc = merge_nproc
    elif function == 'rerun_missing':
        mem = main_memory
        time = main_time
        scratch = main_scratch
        nproc = main_nproc
    elif function == 'missing':
        mem = merge_memory
        time = merge_time
        scratch = merge_scratch
        nproc = 1
    else:
        mem = main_memory
        time = main_time
        scratch = main_scratch
        nproc = main_nproc

    if system == 'bsub':
        resource_string = '-n {} -We {} -R rusage[mem={}] -R rusage[scratch={}] -R span[ptile={}]'.format(nproc, decimal_hours_to_str(time), mem, scratch, nproc)

    return resource_string, nproc


def get_log_filenames(log_dir, job_name, function):
    """
    Builds the filenames of the stdout and stderr log files for a
    given job name and a given function to run.

    :param log_dir: directory where the logs are stored
    :param job_name: Name of the job that will write to the log files
    :param function: Function that will be executed
    :return: filenames for stdout and stderr logs
    """
    job_name_ext = job_name + '_' + function
    stdout_log = os.path.join(log_dir, '{}.o'.format(job_name_ext))
    stderr_log = os.path.join(log_dir, '{}.e'.format(job_name_ext))
    return stdout_log, stderr_log


def get_source_cmd(source_file):
    """
    Builds the command to source a given file if the file exists,
    otherwise returns an empty string.

    :param source_file: path to the (possibly non-existing) source file,
                        can be relative and can contain "~"
    :return: command to source the file if it exists or empty string
    """

    source_file_abs = os.path.abspath(os.path.expanduser(source_file))

    if os.path.isfile(source_file_abs):
        source_cmd = 'source {}; '.format(source_file_abs)
    else:
        LOGGER.info('Source file {} not found, skipping'.format(source_file))
        source_cmd = ''

    return source_cmd


def make_cmd_string(function, source_file, n_cores, tasks, mode, job_name,
                    function_args, exe, main_memory, main_time,
                    main_scratch, main_nproc, preprocess_time, preprocess_memory,
                    preprocess_scratch, preprocess_nproc, merge_memory, merge_time,
                    merge_scratch, merge_nproc, log_dir, dependency,
                    system, main_name='main', log_filenames=[]):
    """
    Creates the submission string which gets submitted to the queing system

    :param function: The name of the function defined in the
                     executable that will be submitted
    :param source_file: A file which gets executed
                        before running the actual function(s)
    :param n_cores: The number of cores that will be requested for the job
    :param tasks: The task string, which will get parsed into the job indices
    :param mode: The mode in which the job will be
                 ran (MPI-job or as a jobarray)
    :param job_name: The name of the job
    :param function_args: The remaining arguments that
                          will be forwarded to the executable
    :param exe: The path of the executable
    :param main_memory: Memory per core to allocate for the main job
    :param main_time: The Wall time requested for the main job
    :param main_scratch: Scratch per core to allocate for the main job
    :param preprocess_memory: Memory per core to allocate for the preprocess job
    :param preprocess_time: The Wall time requested for the preprocess job
    :param preprocess_scratch: Scratch to allocate for the preprocess job
    :param merge_memory: Memory per core to allocate for the merge job
    :param merge_time: The Wall time requested for the merge job
    :param log_dir: log_dir: The path to the log directory
    :param merge_scratch: Scratch to allocate for the merge job
    :param dependency: The dependency string
    :param system: The type of the queing system of the cluster
    :param main_name: name of the main function
    :return: The submission string that wil get submitted to the cluster
    """

    # allocate computing resources
    resource_string, omp_num_threads = make_resource_string(function, 
                                                            main_memory, main_time, main_scratch, main_nproc, 
                                                            preprocess_memory, preprocess_time, preprocess_scratch, preprocess_nproc,
                                                            merge_memory, merge_time, merge_scratch, merge_nproc, 
                                                            system)

    # get the job name for the submission system and the log files
    job_name_ext = job_name + '_' + function
    if len(log_filenames)==0:
        stdout_log, stderr_log = get_log_filenames(log_dir, job_name, function)
    else:
        stdout_log, stderr_log = log_filenames

    # construct the string of arguments passed to the executable
    args_string = ''
    for arg in function_args:
        args_string += arg + ' '

    # make submission string
    source_cmd = get_source_cmd(source_file)

    if system == 'bsub':
        if (mode == 'mpi') & (function == 'main'):
            cmd_string = 'bsub -o {} -e {} -J {} -n {} {} {} \"{} mpirun ' \
                'python -m esub.submit_mpi --log_dir={} ' \
                         '--job_name={} --executable={} --tasks=\'{}\' ' \
                         '--main_name={} {}\"'. \
                format(stdout_log, stderr_log, job_name_ext, n_cores,
                       resource_string, dependency, source_cmd, log_dir,
                       job_name, exe, tasks, main_name, args_string)
        else:
            cmd_string = 'bsub -r -o {} -e {} -J {}[1-{}] {} {} \"{} python ' \
                         '-m esub.submit_jobarray --job_name={} ' \
                         '--source_file={} --main_memory={} --main_time={} ' \
                         '--main_scratch={} --function={} ' \
                         '--executable={} --tasks=\'{}\' --n_cores={} ' \
                         '--log_dir={} --system={} --main_name={} {}\"'. \
                format(stdout_log, stderr_log, job_name_ext, n_cores,
                       resource_string, dependency, source_cmd, job_name,
                       source_file, main_memory, main_time, main_scratch,
                       function, exe, tasks, n_cores, log_dir,
                       system, main_name, args_string)

    return cmd_string


def submit_job(tasks, mode, exe, log_dir, function_args, function='main',
               source_file='', n_cores=1, job_name='job', main_memory=100,
               main_time=1, main_nproc=1, main_scratch=1000, preprocess_memory=100,
               preprocess_time=1, preprocess_scratch=1000, preprocess_nproc=1, merge_memory=100,
               merge_time=1, merge_scratch=1000, merge_nproc=1, dependency='', system='bsub',
               main_name='main', log_filenames=[]):
    """
    Based on arguments gets the submission string and submits it to the cluster

    :param tasks: The task string, which will get parsed into the job indices
    :param mode: The mode in which the job will be ran
                 (MPI-job or as a jobarray)
    :param exe: The path of the executable
    :param log_dir: The path to the log directory
    :param function_args: The remaining arguments that will
                          be forwarded to the executable
    :param function: The name of the function defined in the
                     executable that will be submitted
    :param source_file: A file which gets executed before
                        running the actual function(s)
    :param n_cores: The number of cores that will be requested for the job
    :param job_name: The name of the job
    :param main_memory: Memory per core to allocate for the main job
    :param main_time: The Wall time requested for the main job
    :param main_scratch: Scratch per core to allocate for the main job
    :param preprocess_memory: Memory per core to allocate for the preprocess job
    :param preprocess_time: The Wall time requested for the preprocess job
    :param preprocess_scratch: Scratch to allocate for the preprocess job
    :param merge_memory: Memory per core to allocate for the merge job
    :param merge_time: The Wall time requested for the merge job
    :param merge_scratch: Scratch to allocate for the merge job
    :param dependency: The jobids of the jobs on which this job depends on
    :param system: The type of the queing system of the cluster
    :param main_name: name of the main function
    :return: The jobid of the submitted job
    """

    # get submission string
    cmd_string = make_cmd_string(function, source_file, n_cores, tasks, mode,
                                 job_name, function_args, exe,
                                 main_memory, main_time, main_scratch, main_nproc,
                                 preprocess_time, preprocess_memory, preprocess_scratch, preprocess_nproc,
                                 merge_memory, merge_time, merge_scratch, merge_nproc,
                                 log_dir, dependency, system, main_name, log_filenames)

    LOGGER.info( "####################### Submitting command to queing system #######################")
    LOGGER.info(cmd_string)

    # message the system sends if the
    # maximum number of pendings jobs is reached
    if system == 'bsub':
        msg_limit_reached = 'Pending job threshold reached.'
        pipe_limit_reached = 'stderr'

    # submit
    while True:

        output = dict(stdout=[], stderr=[])

        with subprocess.Popen(shlex.split(cmd_string),
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              bufsize=1,
                              universal_newlines=True) as proc:

            # check for limit concerning maximum number of pending jobs
            for line in getattr(proc, pipe_limit_reached):

                pending_limit_reached = msg_limit_reached in line
                if pending_limit_reached:
                    break
                else:
                    output[pipe_limit_reached].append(line)

            # if the limit has been reached, kill process and sleep
            if pending_limit_reached:
                proc.kill()
                LOGGER.info(TIMEOUT_MESSAGE)
                time.sleep(60 * 30)
                continue

            # read rest of the output
            for line in proc.stdout:
                output['stdout'].append(line)
            for line in proc.stderr:
                output['stderr'].append(line)

            break

    # check if process terminated successfully
    if proc.returncode != 0:
        raise RuntimeError('Running the command \"{}\" failed with'
                           'exit code {}. Error: \n{}'.
                           format(cmd_string, proc.returncode,
                                  '\n'.join(output['stderr'])))

    # get id of submitted job (bsub-only up to now)
    jobid = output['stdout'][-1].split('<')[1]
    jobid = jobid.split('>')[0]
    jobid = int(jobid)

    LOGGER.info("Submitted job and got jobid: {}".format(jobid))

    return jobid




def run_jobchainer_flow(args, executable, function_args, path_finished, log_dir, resources):
    """
    Run the specifications for jobchainer flow.

    :param args: Command line arguments are parsed
    :executable: Executable to be run
    :function_args: Function args to be passed to executable
    :path_finished: file holding finished indices
    :log_dir: directory for logs
    :resources: dict with resources
    """

    def add_dependency(dep_string, new_dep):
        if new_dep!='':
            if dep_string=='':
                dep_string+=new_dep
            else:
                dep_string+=' && {}'.format(new_dep)
        return dep_string
    def finalise_deps(dep_string):
        if dep_string!='':
            dep_string = """-w '{}'""".format(dep_string)
        return dep_string



    list_functions = get_jobchainer_function_flow(args, executable)

    # CASE 1 : run locally
    if (args.mode == 'run'):

        for ii, item_name in enumerate(list_functions):

            dc = list_functions[item_name]

            indices = utils.read_indices_yaml(dc['find'])

            if len(indices)>0:

                if len(indices) == len(range(indices[0],indices[-1]+1)):
                    LOGGER.info("Running on tasks: {} > {}".format(indices[0], indices[-1]))
                else:
                    LOGGER.info("Running on tasks: {}".format(indices))

            else:
                LOGGER.info("Running on tasks: {}".format(indices))

            if dc['fun'] == 'main':

                for index in getattr(executable, 'main')(indices, function_args):
                    LOGGER.info("##################### Finished Task {} #####################".format(index))

            elif dc['fun'] == 'missing':

                indices_missing = getattr(executable, 'missing')(indices, function_args)
                filepath_indices_rerun = utils.get_filepath_indices_rerun(args)
                utils.write_indices_yaml(filepath_indices_rerun, indices_missing)

            else:

                getattr(executable, dc['fun'])(indices, function_args)



    # CASE 2 and 3 : running jobs on cluster (jobarray)
    elif (args.mode == 'jobarray'):

        jobids = collections.OrderedDict()
        print(list_functions)

        for ii, item_name in enumerate(list_functions):

            dc = list_functions[item_name]

            LOGGER.info("Submitting {} job to {} core(s)".format(dc['fun'], dc['ncor']))

            # reset logs
            LOGGER.info('Resetting log files')
            stdout_log, stderr_log = get_log_filenames(log_dir, args.job_name, item_name)
            utils.robust_remove(stdout_log)
            utils.robust_remove(stderr_log)

            dep_string = add_dependency(dep_string='', new_dep=args.dependency)
            if dc['dep'] in jobids:
                dep_string = add_dependency(dep_string=dep_string, new_dep=dc['start'].format(jid=str(jobids[dc['dep']])))
            else:
                if dc['dep']!='':
                    raise Exception('could not find dependency {} for item {}, jobids: {}'.format(dc['dep'], item_name, str(jobids)))
            dep_string = finalise_deps(dep_string)


            jobid = submit_job(tasks=dc['find'], 
                               mode=args.mode, 
                               exe=args.exec, 
                               log_dir=log_dir, 
                               function_args=function_args,
                               function=dc['fun'],
                               source_file=args.source_file,
                               n_cores=dc['ncor'],
                               job_name=args.job_name,
                               dependency=dep_string,
                               system=args.system,
                               log_filenames=[stdout_log, stderr_log],
                               **resources)

            jobids[item_name] = jobid
            # LOGGER.critical("Submitted job {} for function {} as jobid {}".format(item_name, dc['fun'], jobid))
            print("Submitted job {} for function {} as jobid {}".format(item_name, dc['fun'], jobid))


def main(args=None):
    """
    Main function of esub.

    :param args: Command line arguments are parsed
    """

    if args is None:
        args = sys.argv[1:]

    # Make log directory if non existing
    log_dir = os.path.join(os.getcwd(), 'esub_logs')
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
        LOGGER.info('Created directory {}'.format(log_dir))

    # initializing parser
    description = "This is esub an user friendly and flexible tool to " \
                  "submit jobs to a cluster or run them locally"
    parser = argparse.ArgumentParser(description=description, add_help=True)

    resources = dict(main_memory=1000,
                     main_time=4,
                     main_time_per_index=None,
                     main_scratch=2000,
                     main_nproc=1,
                     preprocess_memory=1000,
                     preprocess_time=4,
                     preprocess_scratch=2000,
                     preprocess_nproc=1,
                     merge_memory=1000,
                     merge_time=4,
                     merge_scratch=2000,
                     merge_nproc=1)

    # parse all the submitter arguments
    parser.add_argument('exec', type=str, help='path to the executable (python file '
                        'containing functions main, preprocess, merge)')
    parser.add_argument('--mode', type=str, default='run',
                        choices=('run', 'jobarray', 'mpi',
                                 'run-mpi', 'run-tasks'),
                        help='The mode in which to operate. '
                        'Choices: run, jobarray, mpi, run-mpi, '
                        'run-tasks')
    parser.add_argument('--job_name', type=str, default='job',
                        help='Individual name for this job. CAUTION: '
                             'Multiple jobs with same name'
                             'can confuse system!')
    parser.add_argument('--source_file', type=str, default='activate',
                        help='Optionally provide a source file which '
                        'gets executed first (loading modules, '
                        'declaring environemental variables and so on')
    parser.add_argument('--main_memory', type=float,
                        default=resources['main_memory'],
                        help='Memory allocated per core for main job in MB')
    parser.add_argument('--main_time', type=float,
                        default=resources['main_time'],
                        help='Job run time limit in hours for main job')
    parser.add_argument('--main_time_per_index', type=float,
                        default=resources['main_time_per_index'],
                        help='Job run time limit in hours for main '
                             'job per index, overwrites main_time if set')
    parser.add_argument('--main_scratch', type=float,
                        default=resources['main_scratch'],
                        help='Local scratch for allocated for main job')
    parser.add_argument('--main_nproc', type=float,
                        default=resources['main_nproc'],
                        help='Number of processors for each task for the main job')
    parser.add_argument('--preprocess_memory', type=float,
                        default=resources['preprocess_memory'],
                        help='Memory allocated per core for preprocess job in MB')
    parser.add_argument('--preprocess_time', type=float,
                        default=resources['preprocess_time'],
                        help='Job run time limit in hours for preprocess job')
    parser.add_argument('--preprocess_scratch', type=float,
                        default=resources['preprocess_scratch'],
                        help='Local scratch for allocated for preprocess job')
    parser.add_argument('--preprocess_nproc', type=float,
                        default=resources['preprocess_nproc'],
                        help='Number of processors for each task for the preprocess job')
    parser.add_argument('--merge_memory', type=float,
                        default=resources['merge_memory'],
                        help='Memory allocated per core for merge job in MB')
    parser.add_argument('--merge_time', type=float,
                        default=resources['merge_time'],
                        help='Job run time limit in hours for merge job')
    parser.add_argument('--merge_scratch', type=float,
                        default=resources['merge_scratch'],
                        help='Local scratch for allocated for merge job')
    parser.add_argument('--merge_nproc', type=float,
                        default=resources['merge_nproc'],
                        help='Number of processors for each task for the merge job')
    parser.add_argument('--function', type=str, default=['main'], nargs='+',
                        choices=('main', 'preprocess', 'merge', 'missing', 'rerun_missing', 'all'),
                        help='The functions that should be executed. '
                        'Choices: main, preprocess, merge, rerun_missing, all')
    parser.add_argument('--tasks', type=str, default='0',
                        help='Task string from which the indices are parsed. '
                             'Either single index, list of indices or range '
                             'looking like int1 > int2')
    parser.add_argument('--n_cores', type=int, default=1,
                        help='The number of cores to request')
    parser.add_argument('--dependency', type=str, default='',
                        help='A dependency string that gets added to the '
                             'dependencies (meant for pipelining)')
    parser.add_argument('--system', type=str, default='bsub',
                        choices=('bsub',),
                        help='Type of the queing system '
                             '(so far only know bsub)')
    parser.add_argument('--n_rerun_missing', type=int, default=0,
                        help='Number of reruns of the missing indices')
    parser.add_argument('--merge_depenency_mode', type=str, default='after'),
    args, function_args = parser.parse_known_args(args)

    args.submit_dir = os.getcwd()

    if len(args.function) == 1 and args.function[0] == 'all':
        args.function = 'all'

    # Make sure that executable exits
    if os.path.isfile(args.exec):
        if not os.path.isabs(args.exec):
            args.exec = os.path.join(os.getcwd(), args.exec)
    elif 'SUBMIT_DIR' in os.environ:
        args.exec = os.path.join(os.environ['SUBMIT_DIR'], args.exec)
    else:
        raise FileNotFoundError('Please specify a valid path for executable')

    starter_message()
    LOGGER.info('using executable {}'.format(args.exec))

    # Set path to log file and to file storing finished main job ids
    path_log = utils.get_path_log(log_dir, args.job_name)
    path_finished = utils.get_path_finished_indices(log_dir, args.job_name)
    LOGGER.info("Using log file {}".format(path_log))
    LOGGER.info("Storing finished indices in file {}".format(path_finished))
    LOGGER.info("Running in mode {}".format(args.mode))

    # importing the functions from the executable
    executable = utils.import_executable(args.exec)

    # run setup if implemented
    if hasattr(executable, 'setup'):
        LOGGER.info('Running setup from executable')
        getattr(executable, 'setup')(function_args)

    # get resources from executable if implemented
    res_update = dict()
    if hasattr(executable, 'resources'):
        LOGGER.info('Getting cluster resources from executable')
        res_update = getattr(executable, 'resources')(function_args)

    # overwrite with non-default command-line input
    for res_name, res_default_val in resources.items():
        res_cmd_line = getattr(args, res_name)
        if res_cmd_line != res_default_val:
            res_update[res_name] = res_cmd_line

    resources.update(res_update)

    # get indices

    if not os.path.isfile(args.tasks):
        indices = utils.get_indices(args.tasks)
    else:
        indices = utils.read_indices_yaml(args.tasks)

    filepath_indices = utils.get_filepath_indices(args)
    utils.write_indices_yaml(filepath_indices, indices)
    args.tasks = filepath_indices


    # fix main time

    if resources['main_time_per_index'] is not None:
            n_indices = len(indices)
            resources['main_time'] = resources['main_time_per_index'] * math.ceil(n_indices / args.n_cores)

    del resources['main_time_per_index']

    # execute the function flow

    # if args.jobchainer_flow == True:

    run_jobchainer_flow(args, executable, function_args, path_finished, log_dir, resources)

    # else:

        # run_esub_flow(args, executable, function_args, path_finished, log_dir, path_log, resources)

if __name__ == '__main__':
    main()
