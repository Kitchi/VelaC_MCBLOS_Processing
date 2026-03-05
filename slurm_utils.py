"""Shared SLURM utilities for the Vela MC-BLOS pipeline wrapper.

Functions for modifying sbatch files, submitting jobs, and generating
standalone sbatch scripts. Adapted from the MIGHTEE-pol run_cal.py pattern.
"""

import os
import re
import glob
import subprocess


def replace_lines(batchfile, targetstr, newstr):
    """Replace lines containing targetstr with newstr in a batch file."""
    with open(batchfile) as infile:
        lines = infile.readlines()

    newlines = []
    for line in lines:
        newline = line.rstrip('\n')
        if targetstr in newline:
            newline = newstr
        newlines.append(newline)

    with open(batchfile, 'w') as outfile:
        for line in newlines:
            outfile.write(line + '\n')


def check_existence(batchfile):
    """Check if a batch file exists, print warning if not."""
    if not os.path.exists(batchfile):
        print("Warning: {} does not exist, skipping".format(batchfile))
        return False
    return True


def change_time(batchfile, newtime):
    """Change the --time parameter in an sbatch file."""
    if not check_existence(batchfile):
        return
    timeline = "#SBATCH --time=%s" % newtime
    replace_lines(batchfile, '--time', timeline)


def change_mem(batchfile, mem):
    """Change the --mem parameter in an sbatch file."""
    if not check_existence(batchfile):
        return
    memline = "#SBATCH --mem=%s" % mem
    replace_lines(batchfile, '--mem', memline)


def change_nodes(batchfile, nodes):
    """Change the --nodes parameter in an sbatch file."""
    if not check_existence(batchfile):
        return
    nodeline = "#SBATCH --nodes=%s" % nodes
    replace_lines(batchfile, '--nodes', nodeline)


def change_ntasks_per_node(batchfile, ntasks):
    """Change the --ntasks-per-node parameter in an sbatch file."""
    if not check_existence(batchfile):
        return
    taskline = "#SBATCH --ntasks-per-node=%s" % ntasks
    replace_lines(batchfile, '--ntasks-per-node', taskline)


def change_cpus_per_task(batchfile, ncpus):
    """Change the --cpus-per-task parameter in an sbatch file."""
    if not check_existence(batchfile):
        return
    taskline = "#SBATCH --cpus-per-task=%s" % ncpus
    replace_lines(batchfile, '--cpus-per-task', taskline)


# Map from override key to modifier function
_PARAM_FUNCS = {
    'time': change_time,
    'mem': change_mem,
    'nodes': change_nodes,
    'ntasks_per_node': change_ntasks_per_node,
    'cpus_per_task': change_cpus_per_task,
}


def apply_sbatch_overrides(overrides_dict, spw_dirs=None):
    """Apply sbatch overrides from the [sbatch_overrides] config section.

    overrides_dict maps 'scriptname.param' -> value, e.g.:
        {'partition.time': '06:00:00', 'flag_round_1.mem': '64GB'}

    For scripts that exist per-SPW (like flag_round_1, setjy, etc.),
    the override is applied to both the root-level file (if it exists)
    and to each SPW subdirectory copy.

    spw_dirs: list of SPW directory names (e.g. ['880~930MHz', ...]).
              If None, auto-detected via glob('*MHz').
    """
    if spw_dirs is None:
        spw_dirs = sorted(glob.glob('*MHz'))

    for key, value in overrides_dict.items():
        parts = key.rsplit('.', 1)
        if len(parts) != 2:
            print("Warning: invalid sbatch_overrides key '{}', expected 'script.param'".format(key))
            continue

        script_name, param = parts
        batchfile = '{}.sbatch'.format(script_name)

        if param not in _PARAM_FUNCS:
            print("Warning: unknown sbatch parameter '{}' for '{}'".format(param, script_name))
            continue

        func = _PARAM_FUNCS[param]

        # Apply to root-level sbatch if it exists
        if os.path.exists(batchfile):
            func(batchfile, value)

        # Apply to per-SPW copies
        for spw_dir in spw_dirs:
            spw_batchfile = os.path.join(spw_dir, batchfile)
            if os.path.exists(spw_batchfile):
                func(spw_batchfile, value)


def submit_sbatch(script, dependency=None, dep_type='afterok'):
    """Submit an sbatch script and return the job ID.

    If dependency is provided, adds --dependency=dep_type:dependency.
    Returns the job ID as a string, or None on failure.
    """
    cmd = ['sbatch']
    if dependency is not None:
        cmd.append('--dependency={}:{}'.format(dep_type, dependency))
    cmd.append(script)

    print("Submitting: {}".format(' '.join(cmd)))
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print("Error submitting {}: {}".format(script, result.stderr.strip()))
        return None

    # sbatch output: "Submitted batch job 12345678"
    match = re.search(r'(\d+)', result.stdout)
    if match:
        job_id = match.group(1)
        print("  -> Job ID: {}".format(job_id))
        return job_id

    print("Warning: could not parse job ID from: {}".format(result.stdout.strip()))
    return None


def get_last_job_id(submit_output):
    """Parse submit_pipeline.sh output and return the last job ID.

    submit_pipeline.sh prints lines like:
        Submitted batch job 12345678
    for each step. We want the very last one.
    """
    job_ids = re.findall(r'Submitted batch job (\d+)', submit_output)
    if job_ids:
        return job_ids[-1]
    return None


def write_sbatch_script(path, job_name, command, account, partition='Main',
                        time='01:00:00', mem='16GB', nodes=1,
                        ntasks_per_node=1, output_log=None):
    """Generate a standalone sbatch script.

    Args:
        path: output file path
        job_name: SLURM job name
        command: the shell command(s) to run
        account: SLURM account string
        partition: SLURM partition
        time: wall time limit
        mem: memory limit
        nodes: number of nodes
        ntasks_per_node: tasks per node
        output_log: path for stdout/stderr (defaults to job_name-%j.log)
    """
    if output_log is None:
        output_log = '{}-{}.log'.format(job_name, '%j')

    lines = [
        '#!/bin/bash',
        '#SBATCH --job-name={}'.format(job_name),
        '#SBATCH --account={}'.format(account),
        '#SBATCH --partition={}'.format(partition),
        '#SBATCH --time={}'.format(time),
        '#SBATCH --mem={}'.format(mem),
        '#SBATCH --nodes={}'.format(nodes),
        '#SBATCH --ntasks-per-node={}'.format(ntasks_per_node),
        '#SBATCH --output={}'.format(output_log),
        '#SBATCH --error={}'.format(output_log),
        '',
        command,
        '',
    ]

    with open(path, 'w') as f:
        f.write('\n'.join(lines))

    os.chmod(path, 0o755)
    print("Wrote sbatch script: {}".format(path))
