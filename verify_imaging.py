#!/usr/bin/env python3
"""Verify imaging outputs for a Vela MC-BLOS pipeline run.

Checks for expected image products (*.image, *.pb) in each per-field
imaging directory. Runs as a lightweight SLURM job (no CASA dependency).

Usage:
    python verify_imaging.py --obs-id <obs_id> --work-dir <path>
"""

import argparse
import configparser
import ast
import csv
import glob
import os
import sys
from datetime import datetime


IMAGING_LOG = '/scratch3/projects/vela-mc-blos/imaging_log.csv'


def parse_args():
    parser = argparse.ArgumentParser(description='Verify Vela imaging outputs')
    parser.add_argument('--obs-id', required=True, help='Observation ID')
    parser.add_argument('--work-dir', required=True, help='Path to pipeline working directory')
    return parser.parse_args()


def get_target_fields(config_file):
    """Read target field names from default_config.txt."""
    config = configparser.ConfigParser(allow_no_value=True)
    config.read(config_file)

    if not config.has_section('fields') or not config.has_option('fields', 'targetfields'):
        return []

    raw = config.get('fields', 'targetfields')
    try:
        raw = ast.literal_eval(raw)
    except (ValueError, SyntaxError):
        pass
    raw = str(raw).strip().strip("'\"")
    return [f.strip() for f in raw.split(',') if f.strip()]


def check_imaging_outputs(work_dir, target_fields):
    """Check that each per-field imaging directory has image products.

    Checks are kept generic (glob for *.image, *.pb) rather than
    hardcoding tclean naming, so they remain valid when LibRA replaces
    tclean in the future.
    """
    missing = []

    for field in target_fields:
        field_dir = os.path.join(work_dir, 'imaging_{}'.format(field))

        if not os.path.isdir(field_dir):
            missing.append('imaging_{}/: directory missing'.format(field))
            continue

        # Check for science image products (*.image directories)
        image_products = glob.glob(os.path.join(field_dir, '*.image'))
        if not image_products:
            missing.append('imaging_{}/: no *.image products found'.format(field))

        # Check for primary beam products (*.pb directories, needed for linmos)
        pb_products = glob.glob(os.path.join(field_dir, '*.pb'))
        if not pb_products:
            missing.append('imaging_{}/: no *.pb products found'.format(field))

    return missing


def write_log(obs_id, status, work_dir, missing_items):
    """Append a row to the imaging log CSV."""
    log_dir = os.path.dirname(IMAGING_LOG)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, mode=0o775, exist_ok=True)

    file_exists = os.path.isfile(IMAGING_LOG)

    with open(IMAGING_LOG, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(['obs_id', 'status', 'timestamp', 'work_dir', 'missing_items'])
        writer.writerow([
            obs_id,
            status,
            datetime.now().isoformat(timespec='seconds'),
            work_dir,
            '; '.join(missing_items) if missing_items else '',
        ])


def main():
    args = parse_args()
    obs_id = args.obs_id
    work_dir = os.path.abspath(args.work_dir)

    print("=" * 60)
    print("Vela Imaging Verification")
    print("Obs ID:   {}".format(obs_id))
    print("Work dir: {}".format(work_dir))
    print("=" * 60)

    if not os.path.isdir(work_dir):
        print("ERROR: working directory does not exist: {}".format(work_dir))
        write_log(obs_id, 'FAILURE', work_dir, ['work_dir does not exist'])
        sys.exit(1)

    # Read target fields from config
    config_file = os.path.join(work_dir, 'default_config.txt')
    if os.path.exists(config_file):
        target_fields = get_target_fields(config_file)
        print("Target fields: {}".format(', '.join(target_fields)))
    else:
        print("ERROR: default_config.txt not found in {}".format(work_dir))
        write_log(obs_id, 'FAILURE', work_dir, ['default_config.txt not found'])
        sys.exit(1)

    if not target_fields:
        print("ERROR: no target fields found in config")
        write_log(obs_id, 'FAILURE', work_dir, ['no target fields in config'])
        sys.exit(1)

    # Run checks
    missing = check_imaging_outputs(work_dir, target_fields)

    # Report results
    print("-" * 60)
    if missing:
        status = 'FAILURE'
        print("STATUS: FAILURE")
        print("Missing items:")
        for item in missing:
            print("  - {}".format(item))
    else:
        status = 'SUCCESS'
        print("STATUS: SUCCESS")
        print("All expected imaging outputs found.")

    print("-" * 60)
    write_log(obs_id, status, work_dir, missing)

    sys.exit(0 if status == 'SUCCESS' else 1)


if __name__ == '__main__':
    main()
