from termcolor import colored
import subprocess

def prompt_to_delete():
    delete = input(colored(f"Continue with job? (y/n) ", color='yellow', attrs=['bold']))
    return delete

def abort_message():
    """Display an abort message"""

    print(colored('Aborting...', color='yellow', attrs=['bold']))

def slurm_message(sbatch_filename, wall_time, ntasks, no_submit):
    """Display details related to the Slurm job
    
    Arguments:
        sbatch_filename     name of the sbatch file
        wall_time           wall-time of the task, in hours
        ntasks              number of parallel tasks
        no_submit           if True, do not ask to submit the Slurm job
    """

    print(colored('sbatch file', color='yellow', attrs=['bold']))
    subprocess.run(['cat',  f'{sbatch_filename}'])

    print(colored('\nSlurm job', color='yellow', attrs=['bold']))
    print('    Number of tasks:', colored(f'{ntasks}', attrs=['bold']))
    print('    Max wall-time:', colored(f'{wall_time:.2f} hours', attrs=['bold']))
    print('    Max CPU-hour usage:', colored(f'{ntasks*wall_time:.2f} hours', attrs=['bold']))

    if not no_submit:
        submit_job = input(colored('\nSubmit Slurm job? (y/n) ', color='yellow', attrs=['bold']))
        if submit_job != 'y':
            print(colored('\nNot submitting Slurm job', color='yellow', attrs=['bold']))
            return 

        subprocess.run(['sbatch', sbatch_filename])
        print(colored('\nSlurm job submitted', color='yellow', attrs=['bold']))

def delete_message(filepaths):
    """Display data that is to be deleted
    
    Arguments:
        filepaths    list of filepaths
    """

    summary = colored("The following cached data will be deleted:\n", color='yellow', attrs=['bold'])
    for filepath in filepaths:
        summary += filepath + '\n'

    print(summary)
    delete = prompt_to_delete()
    print('')

    if delete == 'y':
        return True
    else:
        return False

def display_message(cached_functions, instances, instance_functions, at_end_functions):
    """Message to shown when 'display' command is run
    
    Arguments:
    """

    print(colored("cached functions:", color='yellow', attrs=['bold']))
    for name,func in cached_functions.items():
        print('    ', colored(name, color='yellow'), ' -- ', func.__doc__, sep='')

    for base,instance in instances.items():
        print('    ', colored(base, color='yellow'), ' -- ', instance_functions[base].__doc__, sep='')
        print('      ', f'[{len(instance)} instances] ', end='')
        for name, func in instance.items():
            subname = name.split('-')[1]
            print(subname, end=' ')
        print('')

    print(colored("\nat-end functions:", color='yellow', attrs=['bold']))
    for name,func in at_end_functions.items():
        print('    ', colored(name, color='yellow'), ' -- ', func.__doc__, sep='')

def at_end_message():
    """Display message when running at-end functions"""

    print(colored('Running at-end functions', color='yellow'))

def cached_function_message(name):
    """Display message when running cached function
    
    Arguments:
        name    name of the cached function
    """

    print(colored(f"Running cached function '{name}'", color='yellow'))
