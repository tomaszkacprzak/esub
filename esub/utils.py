# Copyright (C) 2019 ETH Zurich,
# Institute for Particle Physics and Astrophysics
# Author: Dominik Zuercher

import os
import time
import sys
import shutil
import logging
import math
import datetime
import subprocess
import shlex
import portalocker
import multiprocessing
from functools import partial
import numpy as np
import yaml

def set_logger_level(logger, level):

    logging_levels = {'critical': logging.CRITICAL,
                      'error': logging.ERROR,
                      'warning': logging.WARNING,
                      'info': logging.INFO,
                      'debug': logging.DEBUG}

    logger.setLevel(logging_levels[level])
    
def get_logger(filepath, logging_level=None):
    """
    Get logger, if logging_level is unspecified, then try using the environment variable PYTHON_LOGGER_LEVEL.
    Defaults to info.
    :param filepath: name of the file that is calling the logger, used to give it a name.
    :return: logger object
    """

    if logging_level is None:
        if 'PYTHON_LOGGER_LEVEL' in os.environ:
            logging_level = os.environ['PYTHON_LOGGER_LEVEL']
        else:
            logging_level = 'info'

    logger_name = '{:>12}'.format(os.path.basename(filepath)[:12])
    logger = logging.getLogger(logger_name)

    if len(logger.handlers) == 0:
        log_formatter = logging.Formatter(fmt="%(asctime)s %(name)0.12s %(levelname).3s   %(message)s ",  datefmt="%y-%m-%d %H:%M:%S", style='%')
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(log_formatter)
        logger.addHandler(stream_handler)
        logger.propagate = False
        set_logger_level(logger, logging_level)

    return logger

LOGGER = get_logger(__file__)


def robust_remove(path):
    """
    Remove a file or directory if existing

    :param path: path to possible non-existing file or directory
    """
    if os.path.isfile(path):
        os.remove(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)


def get_path_log(log_dir, job_name):
    """
    Construct the path of the esub log file

    :param log_dir: directory where log files are stored
    :param job_name: name of the job that will be logged
    :return: path of the log file
    """
    path_log = os.path.join(log_dir, job_name + '.log')
    return path_log


def get_path_finished_indices(log_dir, job_name):
    """
    Construct the path of the file containing the finished indices

    :param log_dir: directory where log files are stored
    :param job_name: name of the job for which the indices will be store
    :return: path of the file for the finished indices
    """
    path_finished = os.path.join(log_dir, job_name + '_done.dat')
    return path_finished


def import_executable(exe, verbose=True):
    """
    Imports the functions defined in the executable file.

    :param exe: path of the executable
    :param verbose: whether to give out a logging statement about the import
    :return: executable imported as python module
    """
    sys.path.insert(0, os.path.dirname(exe))
    to_import = os.path.basename(exe).replace('.py', '')
    executable = __import__(to_import)
    if verbose:
        LOGGER.info('Imported {}'.format(exe))
    return executable


def save_write(path, str_to_write, mode='a'):
    """
    Write a string to a file, with the file being locked in the meantime.

    :param path: path of file
    :param str_to_write: string to be written
    :param mode: mode in which file is opened
    """
    with portalocker.Lock(path, mode=mode, timeout=math.inf) as f:
        # write
        f.write(str_to_write)
        # flush and sync to filesystem
        f.flush()
        os.fsync(f.fileno())


def write_index(index, finished_file):
    """
    Writes the index number on a new line of the
    file containing the finished indices

    :param index: A job index
    :param finished_file: The file to which the
                          jobs will write that they are done
    """
    save_write(finished_file, '{}\n'.format(index))


def check_indices(indices, finished_file, exe, function_args, LOGGER):
    """
    Checks which of the indices are missing in
    the file containing the finished indices

    :param indices: Job indices that should be checked
    :param finished_file: The file from which the jobs will be read
    :param exe: Path to executable
    :return: Returns the indices that are missing
    """
    # wait for the indices file to be written
    while not os.path.exists(finished_file):
        time.sleep(60)

    # first get the indices missing in the log file (crashed jobs)
    done = []
    with open(finished_file, 'r') as f:
        for line in f:
            done.append(int(line.replace('\n', '')))
    failed = list(set(indices) - set(done))

    # if provided use check_missing function
    # (finished jobs but created corrupted output)
    if hasattr(exe, 'check_missing'):
        LOGGER.info("Found check_missing function in executable.")
        corrupted = getattr(exe, 'check_missing')(indices, function_args)
    else:
        corrupted = []

    missing = failed + corrupted
    missing = np.unique(np.asarray(missing))

    return missing


def write_to_log(path, line, mode='a'):
    """
    Write a line to a esub log file

    :param path: path of the log file
    :param line: line (string) to write
    :param mode: mode in which the log file will be opened
    """
    extended_line = "{}    {}\n".format(datetime.datetime.now(), line)
    save_write(path, extended_line, mode=mode)


def cd_local_scratch(verbose=True):
    """
    Change to current working directory to the local scratch if set.

    :param verbose: whether to give out a logging
                    statement about the new working directory.
    """
    if 'ESUB_LOCAL_SCRATCH' in os.environ:

        if os.path.isdir(os.environ['ESUB_LOCAL_SCRATCH']):
            submit_dir = os.getcwd()
            os.chdir(os.environ['ESUB_LOCAL_SCRATCH'])
            os.environ['SUBMIT_DIR'] = submit_dir

            if verbose:
                LOGGER.info('Changed current working directory to {} and '
                            'set $SUBMIT_DIR to {}'.
                            format(os.getcwd(), os.environ['SUBMIT_DIR']))
        else:
            LOGGER.error('$ESUB_LOCAL_SCRATCH is set to non-existing '
                         'directory {}, skipping...'.
                         format(os.environ['ESUB_LOCAL_SCRATCH']))


def run_local_mpi_job(exe, n_cores, function_args, logger, main_name='main'):
    """
    This function runs an MPI job locally

    :param exe: Path to executable
    :param n_cores: Number of cores
    :param function_args: A list of arguments to be passed to the executable
    :param index: Index number to run
    :param logger: logger instance for logging
    :param main_name:
    """
    # construct the string of arguments passed to the executable
    args_string = ''
    for arg in function_args:
        args_string += arg + ' '

    # make command string
    cmd_string = 'mpirun -np {} python -m esub.submit_mpi' \
                 ' --executable={} --tasks=\'0\' --main_name={} {}'.\
                 format(n_cores, exe, main_name, args_string)
    for line in execute_local_mpi_job(cmd_string):
        line = line.strip()
        if len(line) > 0:
            logger.info(line)


def get_indices(tasks):
    """
    Parses the jobids from the tasks string.

    :param tasks: The task string, which will get parsed into the job indices
    :return: A list of the jobids that should be executed
    """
    # parsing a list of indices from the tasks argument

    if os.path.exists(tasks):
        try:
            indices = read_indices_yaml(tasks)
            return indices
        except:
            with open(tasks, 'r') as f:
                content = f.readline()
            indices = get_indices(content)
    elif '>' in tasks:
        tasks = tasks.split('>')
        start = tasks[0].replace(' ', '')
        stop = tasks[1].replace(' ', '')
        indices = list(range(int(start), int(stop)))
    elif ',' in tasks:
        indices = tasks.split(',')
        indices = list(map(int, indices))
    else:
        try:
            indices = [int(tasks)]
        except ValueError:
            raise ValueError("Tasks argument is not in the correct format!")

    return indices


def get_indices_splitted(tasks, n_cores, rank):
    """
    Parses the jobids from the tasks string.
    Performs load-balance splitting of the jobs and returns the indices
    corresponding to rank. This is only used for job array submission.

    :param tasks: The task string, which will get parsed into the job indices
    :param n_cores: The number of cores that will be requested for the job
    :param rank: The rank of the core
    :return: A list of the jobids that should
             be executed by the core with number rank
    """

    # Parse
    indices = get_indices(tasks)

    # Load-balanced splitter
    steps = len(indices)
    size = n_cores
    chunky = int(steps / size)
    rest = steps - chunky * size
    mini = chunky * rank
    maxi = chunky * (rank + 1)
    if rank >= (size - 1) - rest:
        maxi += 2 + rank - size + rest
        mini += rank - size + 1 + rest
    mini = int(mini)
    maxi = int(maxi)

    return indices[mini:maxi]


def function_wrapper(indices, args, func):
    """
    Wrapper that converts a generator to a function.

    :param generator: A generator
    """
    inds = []
    for ii in func(indices, args):
        inds.append(ii)
    return inds


def run_local_mpi_tasks(exe, n_cores, function_args, tasks, function, logger):
    """
    Executes an MPI job locally, running each splitted index list on one core.

    :param exe: The executable from where the main function is imported.
    :param n_cores: The number of cores to allocate.
    :param function_args: The arguments that
                          will get passed to the main function.
    :param tasks: The indices to run on.
    :param function: The function name to run
    :param logger: The logger instance
    """
    # get executable
    func = getattr(exe, function)

    # Fix function arguments for all walkers
    run_func = partial(function_wrapper, args=function_args, func=func)

    # get splitted indices
    nums = []
    for rank in range(n_cores):
        nums.append(get_indices_splitted(tasks, n_cores, rank))

    # Setup mutltiprocessing pool
    pool = multiprocessing.Pool(processes=n_cores)
    if int(multiprocessing.cpu_count()) < n_cores:
        raise Exception(
            "Number of CPUs available is smaller \
             than requested number of CPUs")

    # run and retrive the finished indices
    out = pool.map(run_func, nums)
    out = [item for sublist in out for item in sublist]
    return out


def execute_local_mpi_job(cmd_string):
    """
    Execution of local MPI job

    :param cmd_string: The command string to run
    """
    popen = subprocess.Popen(shlex.split(cmd_string),
                             stdout=subprocess.PIPE, universal_newlines=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd_string)

def get_dirname_indices(args):

    indices_dir = os.path.join(args.submit_dir, 'esub_indices')
    if not os.path.isdir(indices_dir):
        os.makedirs(indices_dir)
        LOGGER.info('Created directory {}'.format(indices_dir))
    args.indices_dir = indices_dir
    return indices_dir


def get_filepath_indices(args):

    dirpath_indices = get_dirname_indices(args)
    filename_indices = 'indices__{}.yaml'.format(args.job_name)
    filepath_indices = os.path.join(dirpath_indices, filename_indices)
    return filepath_indices

def get_filepath_indices_rerun(args):

    # dirpath_indices = get_dirname_indices(args)
    # filename_indices = 'indices_rerun__{}.yaml'.format(args.job_name)
    # filepath_indices = os.path.join(dirpath_indices, filename_indices)
    filepath_indices = args.tasks.replace('.yaml', '_rerun.yaml')

    return filepath_indices

def write_indices_yaml(filepath_indices, indices):

    with open(filepath_indices, 'w') as fout:
        yaml.dump(indices, fout)
    LOGGER.info('wrote {} with {} indices'.format(filepath_indices, len(indices)))


def read_indices_yaml(filepath_indices):

    with open(filepath_indices, 'r') as fin:
        indices = yaml.load(fin, Loader=yaml.FullLoader)
    LOGGER.info('read {} with {} indices'.format(filepath_indices, len(indices)))
    return indices


def isolate_function(funcname, filename, fname_temp_module='temp_module'):

    def get_fuction_source_code(funcname, source_code):

        func_found = False
        for i, line in enumerate(source_code):
            if f'def {funcname}' in line:
                LOGGER.debug(i, line)
                first_line = i
                func_found = True
                break

        assert func_found, f'function {funcname} not found in {filename}'

        for i in range(first_line, len(source_code)):

            line = source_code[i]

            if 'return' in line:
                LOGGER.debug(i, line)
                last_line = i+1
                break

        return source_code[first_line:last_line]

    def write_module_with_source(filename, source_code):

        with open(f'{filename}.py', 'w') as f:
            f.writelines(source_code)

    with open(filename, 'r') as f:
        source_code = f.readlines()

    func_source_code = get_fuction_source_code(funcname=funcname, source_code=source_code)
    write_module_with_source(filename=fname_temp_module, source_code=func_source_code)


    import importlib
    mymodule = importlib.import_module(fname_temp_module)
    func = getattr(mymodule, funcname)
    os.remove(f'{fname_temp_module}.py')
    return func

def get_module_functions_noimport(filename):

    with open(filename, 'r') as f:
        source_code = f.readlines()
    
    list_functions = []
    for line in source_code:
        if line.startswith('def '):
            funcname = line.split('def ')[1].split('(')[0]
            LOGGER.debug(f'found func {funcname} in  {filename}')
            list_functions.append(funcname)

    return list_functions

