import os

def wall_time(time_str):
    """return the wall-time for a given run-time string (in hours)"""
    time_str = str(time_str)
    split = time_str.split(':')

    hours = int(split[0].zfill(1))
    if len(split) > 1:
        hours += int(split[1].zfill(1))/60
    if len(split) > 2:
        hours += int(split[2].zfill(1))/60**2

    return hours

def format_time(time_str):
    """
    Properly format a run-time string for the sbatch file 
    
    Examples:
        15       ->   15:00:00
        2:30:5   ->   02:30:05
        :30      ->   00:30:00
        ::30     ->   00:00:30
    """
    time_str = str(time_str)
    split = time_str.split(':')
    hours = split[0].zfill(2)

    if len(split) > 1:
        minutes = split[1].zfill(2)
    else:
        minutes = '00'

    if len(split) > 2:
        seconds = split[2].zfill(2)
    else:
        seconds = '00'

    return ':'.join([hours, minutes, seconds])

# def create_sbatch(py_filename, func_names, time='36', memory=2, partition='broadwl'):
    # """create an sbatch file to run the provided functions
    
    # Arguments:
        # py_filename     name of the Python filen
        # func_names      list of function names to be executed
        # time            string representation of run-time, {hours}:{minutes}:{seconds}
        # memory          memory per cpu, in GB
        # partition       name of partition to run on
    # """
    # ntasks = len(func_names)
    # r_flag = ' '.join(list(func_names))
    # mem_in_mb = int(memory*1000)
    # time_str = format_time(time)

    # os.makedirs('output', exist_ok=True)

    # output = f"""#!/bin/bash                                                                

# #SBATCH --job-name={py_filename}
# #SBATCH --partition={partition}
# #SBATCH --ntasks={ntasks}
# #SBATCH --time={time_str}
# #SBATCH --mem-per-cpu={mem_in_mb}

# python {py_filename}.py -r {r_flag} -p {ntasks} --no-at-end > output/{py_filename}.log 2> output/{py_filename}.err
# """
    
    # sbatch_filename = f'{py_filename}.sbatch'
    # with open(sbatch_filename, 'w') as f:
        # f.write(output)

    # return sbatch_filename

def create_lookup(py_filename, func_names):
    with open(f'{py_filename}-lookup.txt', 'w') as f:
        for func_name in func_names:
            f.write(f'{func_name}\n')

def create_sbatch(py_filename, func_names, time='36', memory=2, partition='broadwl'):
    ntasks = len(func_names)
    mem_in_mb = int(memory*1000)
    time_str = format_time(time)
    output_dir = f'{py_filename}_output'
    runtask_log = f'{py_filename}_runtask.log'

    os.makedirs(output_dir, exist_ok=True)

    output = f"""#!/bin/sh

#SBATCH --job-name={py_filename}
#SBATCH --partition={partition}
#SBATCH --ntasks={ntasks}
#SBATCH --time={time_str}
#SBATCH --mem-per-cpu={mem_in_mb}

ulimit -u 10000
srun="srun --exclusive -N1 -n1"
parallel="parallel --delay .2 -j $SLURM_NTASKS --joblog {runtask_log} --resume"

$parallel "$srun python {py_filename}.py -r {{1}} -p 1 --no-at-end --mininterval 60 > {output_dir}/out_{{1}}.txt 2> {output_dir}/out_{{1}}.err" :::: {py_filename}-lookup.txt
"""
    
    sbatch_filename = f'{py_filename}.sbatch'
    with open(sbatch_filename, 'w') as f:
        f.write(output)

    return sbatch_filename
