#!/usr/bin/env python3
"""Verify calibration outputs for a Vela MC-BLOS pipeline run.

Checks for expected caltables, MMS outputs, and logs results to a CSV.
Runs as a lightweight SLURM job (no CASA dependency).

Usage:
    python verify_calibration.py --obs-id <obs_id> --cal-dir <path>
"""

import argparse
import configparser
import ast
import csv
import glob
import os
import sys
from datetime import datetime


CALIBRATION_LOG = '/scratch3/projects/vela-mc-blos/calibration_log.csv'
EXPECTED_CALTABLE_EXTENSIONS = ['.kcal', '.bcal', '.gcal', '.pcal', '.xcal', '.fluxscale']


def parse_args():
    parser = argparse.ArgumentParser(description='Verify Vela calibration outputs')
    parser.add_argument('--obs-id', required=True, help='Observation ID')
    parser.add_argument('--cal-dir', required=True, help='Path to calibration working directory')
    return parser.parse_args()


def read_config_fields(config_file):
    """Read target and calibrator field names from default_config.txt."""
    config = configparser.SafeConfigParser(allow_no_value=True)
    config.read(config_file)

    fields = []
    if config.has_section('fields'):
        for key in ['bpassfield', 'fluxfield', 'phasecalfield', 'targetfields', 'extrafields']:
            if config.has_option('fields', key):
                try:
                    val = ast.literal_eval(config.get('fields', key))
                except (ValueError, SyntaxError):
                    val = config.get('fields', key)
                if val:
                    # targetfields can be comma-separated
                    for f in str(val).split(','):
                        f = f.strip()
                        if f and f not in fields:
                            fields.append(f)
    return fields


def check_caltables(cal_dir):
    """Check that each SPW directory has the expected caltable files."""
    missing = []
    spw_dirs = sorted(glob.glob(os.path.join(cal_dir, '*MHz')))

    if not spw_dirs:
        missing.append('no *MHz SPW directories found')
        return missing

    for spw_dir in spw_dirs:
        caltables_dir = os.path.join(spw_dir, 'caltables')
        spw_name = os.path.basename(spw_dir)

        if not os.path.isdir(caltables_dir):
            missing.append('{}/caltables/ directory missing'.format(spw_name))
            continue

        caltable_files = os.listdir(caltables_dir)
        for ext in EXPECTED_CALTABLE_EXTENSIONS:
            found = any(f.endswith(ext) for f in caltable_files)
            if not found:
                missing.append('{}/caltables/*{} missing'.format(spw_name, ext))

    return missing


def check_mms_outputs(cal_dir, fields):
    """Check that MMS files exist for expected fields."""
    missing = []

    # Look for .mms files in the cal directory root
    mms_files = glob.glob(os.path.join(cal_dir, '*.mms'))
    mms_basenames = [os.path.basename(m) for m in mms_files]

    if not mms_files:
        missing.append('no .mms files found in cal dir root')
        return missing

    # Check that at least target fields have MMS outputs
    for field in fields:
        found = any(field in name for name in mms_basenames)
        if not found:
            missing.append('no .mms file containing field "{}"'.format(field))

    return missing


def write_log(obs_id, status, cal_dir, missing_items):
    """Append a row to the calibration log CSV."""
    log_dir = os.path.dirname(CALIBRATION_LOG)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, mode=0o775, exist_ok=True)

    file_exists = os.path.isfile(CALIBRATION_LOG)

    with open(CALIBRATION_LOG, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(['obs_id', 'status', 'timestamp', 'cal_dir', 'missing_items'])
        writer.writerow([
            obs_id,
            status,
            datetime.now().isoformat(timespec='seconds'),
            cal_dir,
            '; '.join(missing_items) if missing_items else '',
        ])


def main():
    args = parse_args()
    obs_id = args.obs_id
    cal_dir = os.path.abspath(args.cal_dir)

    print("=" * 60)
    print("Vela Calibration Verification")
    print("Obs ID:  {}".format(obs_id))
    print("Cal dir: {}".format(cal_dir))
    print("=" * 60)

    if not os.path.isdir(cal_dir):
        print("ERROR: calibration directory does not exist: {}".format(cal_dir))
        write_log(obs_id, 'FAILURE', cal_dir, ['cal_dir does not exist'])
        sys.exit(1)

    # Read field names from config
    config_file = os.path.join(cal_dir, 'default_config.txt')
    if os.path.exists(config_file):
        fields = read_config_fields(config_file)
        print("Expected fields: {}".format(', '.join(fields)))
    else:
        fields = []
        print("Warning: default_config.txt not found, skipping MMS field check")

    # Run checks
    missing = []
    missing.extend(check_caltables(cal_dir))
    if fields:
        missing.extend(check_mms_outputs(cal_dir, fields))

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
        print("All expected calibration outputs found.")

    print("-" * 60)
    write_log(obs_id, status, cal_dir, missing)

    sys.exit(0 if status == 'SUCCESS' else 1)


if __name__ == '__main__':
    main()
