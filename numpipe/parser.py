"""
Defines the argument parser
"""
import argparse
from numpipe import config

notifications_default_delay = config.get_config()['notifications']['default_delay']

def run_parser():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="action")
    display_parser = subparsers.add_parser('display', help='display available functions and descriptions')
    display_parser = subparsers.add_parser('clean', help='remove all h5files that are no longer cache functions')
    slurm_parse = subparsers.add_parser('slurm', help='run on a system with the Slurm Workload Manager')

    for p in [parser, slurm_parse]:
        p.add_argument('-r', '--rerun', nargs='*', type=str, default=None, help='re-run specific cached functions by name')
        p.add_argument('-f', '--force', action='store_true', help='force over-write any existing cached data')
        p.add_argument('-d', '--delete', nargs='*', type=str, default=None, help='delete specified cached data')
        p.add_argument('--at-end', action='store_true', default=False, help="only run at_end functions")
        p.add_argument('--no-at-end', action='store_true', default=False, help="don't run at_end functions")
        p.add_argument('-p', '--processes', nargs='?', default=1, type=int, help='number of processes to use in parallel execution (default: cpu_count)')
        p.add_argument('-ct', '--cache_time', type=float, default=300, help='time (in seconds) until data cached data is flushed to file')
        p.add_argument('--tqdm', type=float, default=.1, help='time (in seconds) for tqdm mininterval argument')
        p.add_argument('--notify-delay', type=float, default=notifications_default_delay, help='time (in seconds) before notifications will be sent')

        p.add_argument('--theme', default='normal', type=str, help='matplotlib plot theme')
        p.add_argument('--save', nargs='?', default='', type=str, help='save all figures')
        # p.add_argument('--theme', choices=['classic', 'dark'], default='classic', help='matplotlib plot theme')


    slurm_parse.add_argument('-t', '--time', type=str, default='36', help='maximum run-time for the Slurm job, formated as {hours}:{minutes}:{seconds} (minutes and seconds optional)')
    slurm_parse.add_argument('-m', '--memory', type=float, default=2, help='maximum memory per cpu for the Slurm job in GB')
    slurm_parse.add_argument('--batch', action='store_true', help='submit the Slurm job in batches')
    slurm_parse.add_argument('--no-submit', action='store_true', help="don't submit the Slurm job after creating sbatch files")

    return parser.parse_args()
