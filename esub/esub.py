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
TIMEOUT_MESSAGE = 'Maximum number of pending jobs reached, will sleep for 30 minutes and retry'
RESOURCES_DEFAULT = dict(main_memory=1000,
                         main_time=4,
                         main_time_per_index=None,
                         main_scratch=2000,
                         main_nproc=1,
                         main_ngpu=0,
                         preprocess_memory=1000,
                         preprocess_time=4,
                         preprocess_scratch=2000,
                         preprocess_nproc=1,
                         preprocess_ngpu=0,
                         merge_memory=1000,
                         merge_time=4,
                         merge_scratch=2000,
                         merge_nproc=1,
                         merge_ngpu=0)


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
    parser.add_argument('--function', type=str, default=['main'], nargs='+',
                        choices=('main', 'preprocess', 'merge', 'missing', 'rerun_missing', 'all', 'main+merge'),
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
    parser.add_argument('--system', type=str, default='detect',
                        choices=('detect', 'lsf', 'slurm'),
                        help='Type of the queing system '
                             '(so far only know lsf, slurm)')
    parser.add_argument('--n_rerun_missing', type=int, default=0,
                        help='Number of reruns of the missing indices')
    parser.add_argument('--merge_depenency_mode', type=str, default='after')
    parser.add_argument('--batch_args_pass', type=str, default=None)

    args, function_args = parser.parse_known_args(args)

    args.submit_dir = os.getcwd()

    if len(args.function) == 1:
        if args.function[0] == 'all':  args.function = 'all'
        if args.function[0] == 'main+merge':  args.function = 'main+merge'

    # Make sure that executable exits
    if os.path.isfile(args.exec):
        if not os.path.isabs(args.exec):
            args.exec = os.path.join(os.getcwd(), args.exec)
    elif 'SUBMIT_DIR' in os.environ:
        args.exec = os.path.join(os.environ['SUBMIT_DIR'], args.exec)
    else:
        raise FileNotFoundError('Please specify a valid path for executable')

    starter_message()
    set_batch_system(args)
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

    # get indices
    indices = utils.read_indices_yaml(args.tasks) if os.path.isfile(args.tasks) else utils.get_indices(args.tasks)

    # resources - copy defaults
    resources = get_resources(args, executable, function_args, indices)

    # store indices as file
    filepath_indices = utils.get_filepath_indices(args)
    utils.write_indices_yaml(filepath_indices, indices)
    args.tasks = filepath_indices

    # execute the function flow
    run_jobchainer_flow(args, executable, function_args, path_finished, log_dir, resources)


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
                
def get_resources(args, executable, function_args, indices):

        # resources - copy defaults
    resources = {k:v for k,v in RESOURCES_DEFAULT.items()}

    # get resources from executable if implemented
    if hasattr(executable, 'resources'):
        LOGGER.info('Getting cluster resources from executable')
        res_update = getattr(executable, 'resources')(function_args)
        resources.update(res_update)

    # overwrite with non-default command-line input
    for res_name, res_default_val in resources.items():
        if hasattr(args, res_name):
            resources[res_name] = getattr(args, res_name)

    # fix main time
    if resources['main_time_per_index'] is not None:
            n_indices = len(indices)
            resources['main_time'] = resources['main_time_per_index'] * math.ceil(n_indices / args.n_cores)

    del resources['main_time_per_index']

    # add defaults
    resources.setdefault('main_nsimult', None)
    resources.setdefault('pass', {})

    # add pass args
    if args.batch_args_pass is not None:
        for arg in args.batch_args_pass.split(','):
            key, val = arg.split('=')
            resources['pass'][key] = val
            LOGGER.debug(f'added resource {key}={val}')
    return resources


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

def get_depstr_formats(system):

    depstr = {}

    if system == 'lsf':
        depstr['ended_all'] = """numended({jid:s},*)"""
        depstr['ended_run_any'] = """(numended({jid:s}, > 0) || numrun({jid:s}, > 0))"""

    elif system == 'slurm':

        depstr['ended_all'] = """afterany:{jid:s}"""
        depstr['ended_run_any'] = """after:{jid:s}"""

    return depstr


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
            list_functions['missing{:d}'.format(nr)] = {'fun': 'missing',  'dep': 'main{:d}'.format(nr-1), 'start': depstr['ended_all'], 'ncor': 1, 'find': filepath_indices}
            filepath_indices_rerun = utils.get_filepath_indices_rerun(args)
            list_functions['main{:d}'.format(nr)] =   {'fun': 'main',     'dep': 'missing{:d}'.format(nr), 'start': depstr['ended_all'], 'ncor': n_cores_rerun, 'find': filepath_indices_rerun}

    depstr = get_depstr_formats(args.system)
    list_functions = collections.OrderedDict()

    filepath_indices = utils.get_filepath_indices(args)
    indices = utils.read_indices_yaml(filepath_indices)

    if hasattr(executable, 'preprocess'):
        main_dep = 'preprocess0'
        main_start = depstr['ended_all']
    else:
        main_dep = ''
        main_start = ''

    if args.merge_depenency_mode == 'after':
        merge_start = depstr['ended_all']
        if args.n_rerun_missing==0:
            merge_dep = 'main0'
        else:
            merge_dep = 'main{:d}'.format(args.n_rerun_missing)

    elif args.merge_depenency_mode == 'along':
        merge_start = depstr['ended_run_any']
        merge_dep = 'main0'
    else:
        raise Exception('unknown args.merge_depenency_mode={}'.format(args.merge_depenency_mode))

    if args.function == 'all':

        if hasattr(executable, 'preprocess'):
            list_functions['preprocess0'] = {'fun': 'preprocess',  'dep': '', 'start': '', 'ncor': 1, 'find': filepath_indices}
        list_functions['main0']  = {'fun': 'main',   'dep': main_dep, 'start': main_start, 'ncor': args.n_cores, 'find': filepath_indices}
        add_reruns(args.n_rerun_missing)
        list_functions['merge0'] = {'fun': 'merge',  'dep': merge_dep, 'start': merge_start, 'ncor': 1, 'find': filepath_indices}
    
    elif args.function == 'main+merge':
        list_functions['main0']  = {'fun': 'main',   'dep': '', 'start': '', 'ncor': args.n_cores, 'find': filepath_indices}
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




def make_resource_string(function, resources, system):
    """
    Creates the part of the submission string which handles
    the allocation of ressources
    :param function: The name of the function defined
                     in the executable that will be submitted
    :param resources: Dict with resources
    :param system: The type of the queing system of the cluster
    :return: A string that is part of the submission string.
    """

    if function == 'main':
        mem = resources['main_memory']
        time = resources['main_time']
        scratch = resources['main_scratch']
        nproc = resources['main_nproc']
        ngpu = resources['main_ngpu']
        str_nsimult = ''  if resources['main_nsimult'] is None else f"%{resources['main_nsimult']}"

    elif function == 'preprocess':
        mem = resources['preprocess_memory']
        time = resources['preprocess_time']
        scratch = resources['preprocess_scratch']
        nproc = resources['preprocess_nproc']
        str_nsimult = None
    
    elif function == 'merge':
        mem = resources['merge_memory']
        time = resources['merge_time']
        scratch = resources['merge_scratch']
        nproc = resources['merge_nproc']
        ngpu = resources['merge_ngpu']
        str_nsimult = None
    
    elif function == 'rerun_missing':
        mem = resources['main_memory']
        time = resources['main_time']
        scratch = resources['main_scratch']
        nproc = resources['main_nproc']
        ngpu = resources['main_ngpu']
        str_nsimult = resources['main_nsimult']
    
    elif function == 'missing':
        mem = resources['merge_memory']
        time = resources['merge_time']
        scratch = resources['merge_scratch']
        str_nsimult = None
        ngpu = 1
        nproc = 1
    
    else:
        mem = resources['main_memory']
        time = resources['main_time']
        scratch = resources['main_scratch']
        nproc = resources['main_nproc']
        ngpu = resources['main_ngpu']
        str_nsimult = None

    if system == 'lsf':
        str_resources = f'-n {nproc} '\
                          f'-We {decimal_hours_to_str(time)} '\
                          f'-R rusage[mem={mem}] '\
                          f'-R rusage[scratch={scratch}] '\
                          f'-R span[ptile={nproc}]'

    elif system == 'slurm':
        str_resources = f' --time={int(time*60)} --mem-per-cpu={mem} --cpus-per-task={nproc} ' # in minutes
        if str(ngpu) != '0':
            str_resources += f'--gpus={ngpu} '\
                               f'--gpus-per-task={ngpu} '

    for k, v in resources['pass'].items():
        str_resources += f' --{k}={v} '

    return str_resources, nproc, str_nsimult


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

def clear_logs(stdout_log, stderr_log):

    from glob import glob
    logs = glob(f'{stdout_log}*') + glob(f'{stderr_log}*')
    LOGGER.info(f'clearing {len(logs)} log files')
    for f in logs:
        utils.robust_remove(f)


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
                    function_args, exe, resources, log_dir, dependency,
                    system, log_filenames=[]):
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
    :param resources: Dict with resources
    :param log_dir: log_dir: The path to the log directory
    :param dependency: The dependency string
    :param system: The type of the queing system of the cluster
    :return: The submission string that wil get submitted to the cluster
    """

    # allocate computing resources
    str_resources, omp_num_threads, str_nsimult = make_resource_string(function, resources, system)

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

    if system == 'lsf':
        if (mode == 'mpi') & (function == 'main'):
            cmd_string = f'bsub'\
                         f'-o {stdout_log} '\
                         f'-e {stderr_log} '\
                         f'-J {job_name_ext} '\
                         f'-n {n_cores} '\
                         f'{str_resources} {dependency} \"{source_cmd} ' \
                         f'mpirun python -m esub.submit_mpi '\
                         f'--log_dir={log_dir} ' \
                         f'--job_name={job_name} '\
                         f'--executable={exe} '\
                         f'--tasks=\'{tasks}\' ' \
                         f'{args_string}\"'
        else:

            cmd_string = f'bsub -r '\
                         f'-o {stdout_log} '\
                         f'-e {stderr_log} '\
                         f'-J {job_name_ext}[1-{n_cores}{str_nsimult}] '\
                         f'{str_resources} {dependency} \"{source_cmd} ' \
                         f'python -m esub.submit_jobarray '\
                         f'--job_name={job_name} '\
                         f'--function={function} '\
                         f'--executable={exe} '\
                         f'--tasks=\'{tasks}\' '\
                         f'--n_cores={n_cores} ' \
                         f'--system={system} '\
                         f'{args_string}\"'

    elif system == 'slurm':

        if (mode == 'mpi') & (function == 'main'):
            cmd_string = 'sbatch {} submit.slurm'.format(dependency)

            extra_args = f'--job_name={job_name} --executable={exe} --tasks=\'{tasks}\' --log_dir={log_dir} --main_name={main_name} {args_string}'

            with open('submit.slurm', 'w+') as f:
                f.write('#! /bin/bash \n#\n')
                f.write(f'#SBATCH --output={stdout_log}.%a \n')
                f.write(f'#SBATCH --error={stderr_log}.%a \n')
                f.write(f'#SBATCH --job-name={job_name_ext} \n')
                f.write(f'#SBATCH --ntasks={n_cores} \n')
                f.write(str_resources)
                if len(source_cmd) > 0: f.write(f'srun {source_cmd} \n')
                f.write(f'srun mpirun python -m esub.submit_jobarray {source_cmd} {extra_args}')
        else:
            cmd_string = 'sbatch {} submit.slurm'.format(dependency)

            extra_args = f'--job_name={job_name} '\
                         f'--function={function} '\
                         f'--executable={exe} '\
                         f'--tasks=\'{tasks}\' '\
                         f'--n_cores={n_cores} ' \
                         f'--system={system} '\
                         f'{args_string}'

            with open('submit.slurm', 'w+') as f:
                f.write(f'#! /bin/bash \n#\n')
                f.write(f'#SBATCH --output={stdout_log}.%a \n')
                f.write(f'#SBATCH --error={stderr_log}.%a \n')
                f.write(f'#SBATCH --job-name={job_name_ext} \n')
                f.write(f'#SBATCH --ntasks=1 \n')
                f.write(f'#SBATCH --array=0-{n_cores-1}{str_nsimult} \n')
                for r in str_resources.split(' '):
                    if len(r.strip())>0:
                        f.write(f'#SBATCH {r} \n')
                if len(source_cmd) > 0: f.write(f'{source_cmd} \n')
                f.write(f'python -m esub.submit_jobarray {extra_args} \n')


    return cmd_string


def submit_job(tasks, mode, exe, log_dir, function_args, function='main', source_file='', n_cores=1, 
               job_name='job', resources=RESOURCES_DEFAULT, dependency='', system='lsf', log_filenames=[]):

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
    :param resources: Dict with resources
    :param dependency: The jobids of the jobs on which this job depends on
    :param system: The type of the queing system of the cluster
    :return: The jobid of the submitted job
    """

    # get submission string
    cmd_string = make_cmd_string(function, source_file, n_cores, tasks, mode,
                                 job_name, function_args, exe, resources,
                                 log_dir, dependency, system, log_filenames)

    LOGGER.info( "####################### Submitting command to queing system #######################")
    LOGGER.info(cmd_string)

    # message the system sends if the
    # maximum number of pendings jobs is reached
    if system == 'lsf':
        msg_limit_reached = 'Pending job threshold reached.'
        pipe_limit_reached = 'stderr'
    elif system == 'slurm':
        # TODO: to be filled in for slurm
        msg_limit_reached = 'Pending job threshold reached.'
        pipe_limit_reached = 'stderr'

    # submit
    while True:

        output = dict(stdout=[], stderr=[])

        print(cmd_string)

        with subprocess.Popen(shlex.split(cmd_string),
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              bufsize=1,
                              universal_newlines=True) as proc:

            pending_limit_reached = False
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
        raise RuntimeError('Running the command \"{}\" failed with exit code {}. Error: \n{}'.format(cmd_string, proc.returncode, '\n'.join(output['stderr'])))

    # get id of submitted job (bsub-only up to now)
    if system == 'lsf':
        jobid = output['stdout'][-1].split('<')[1]
        jobid = jobid.split('>')[0]
        jobid = int(jobid)
    elif system == 'slurm':
        jobid = int(output['stdout'][-1].strip().split(' ')[3])
        
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

    def add_dependency(dep_string, new_dep, system):

        if system == 'lsf':
            if new_dep != '':
                if dep_string == '':
                    dep_string += new_dep
                else:
                    dep_string += ' && {}'.format(new_dep)
        
        elif system=='slurm':
            if new_dep != '':
                if dep_string == '':
                    dep_string += f' --dependency={new_dep}'

        return dep_string

    def finalise_deps(dep_string, system):

        if dep_string!='':
            if system=='lsf':
                dep_string = """-w '{}'""".format(dep_string)

            elif system=='slurm':
                pass

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
            print(dc, args.dependency)
            LOGGER.info("Submitting {} job to {} core(s) dep: {}".format(dc['fun'], dc['ncor'], dc['dep']))

            # reset logs
            LOGGER.info('Resetting log files')
            stdout_log, stderr_log = get_log_filenames(log_dir, args.job_name, item_name)
            clear_logs(stdout_log, stderr_log)

            dep_string = add_dependency(dep_string='', new_dep=args.dependency, system=args.system)
            if dc['dep'] in jobids:
                dep_string = add_dependency(dep_string=dep_string, new_dep=dc['start'].format(jid=str(jobids[dc['dep']])), system=args.system)
            else:
                if dc['dep']!='':
                    raise Exception('could not find dependency {} for item {}, jobids: {}'.format(dc['dep'], item_name, str(jobids)))
            dep_string = finalise_deps(dep_string, system=args.system)


            jobid = submit_job(tasks=dc['find'], 
                               mode=args.mode, 
                               exe=args.exec, 
                               log_dir=log_dir, 
                               function_args=function_args,
                               function=dc['fun'],
                               source_file=args.source_file,
                               n_cores=dc['ncor'],
                               job_name=args.job_name,
                               resources=resources,
                               dependency=dep_string,
                               system=args.system,
                               log_filenames=[stdout_log, stderr_log])

            jobids[item_name] = jobid
            # LOGGER.critical("Submitted job {} for function {} as jobid {}".format(item_name, dc['fun'], jobid))
            print("Submitted job {} for function {} as jobid {}".format(item_name, dc['fun'], jobid))

def set_batch_system(args):

    if args.system == 'detect':

        if 'BATCH_SYSTEM' in os.environ:
            args.system = os.environ['BATCH_SYSTEM'].lower()
            LOGGER.info(f'batch system {args.system}')
        else:
            raise Exception('unable to auto-detect the batch system, set env variable BATCH_SYSTEM to lsf or slurm')




if __name__ == '__main__':
    main()
