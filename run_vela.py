#!/usr/bin/env python3
"""Vela MC-BLOS pipeline wrapper.

Automates the IDIA MeerKAT calibration and imaging pipeline:
  1. Find raw MS
  2. Create working directory
  3. Build config via processMeerKAT
  4. Apply modifier overrides
  5. Generate sbatch scripts
  6. Apply sbatch overrides
  7. Submit calibration
  8. Submit calibration verification job (afterany:last_cal_job)
  9. Submit imaging jobs (afterok:verify_cal_job)
 10. Submit imaging verification job (afterany:all_imaging_jobs)
 11. Submit linmos mosaicking job (afterok:verify_imaging_job)
 12. Submit image-move job (afterany:linmos_job)

Usage:
    ./run_vela.py -i <obs_id> [--setup-only] [--skip-imaging] [--force] [--config <modifier.ini>]
"""

import argparse
import ast
import configparser
import glob
import os
import re
import subprocess
import sys

# Add pipeline to path for config_parser
sys.path.insert(0, '/idia/software/pipelines/master/processMeerKAT')

from slurm_utils import (
    apply_sbatch_overrides,
    get_last_job_id,
    submit_sbatch,
    write_sbatch_script,
)

# Paths
RAW_BASE = '/idia/projects/vela-mc-blos/raw/SCI-20251102-MT-01'
SCRATCH_BASE = '/scratch3/projects/vela-mc-blos'
IMAGES_BASE = '/idia/projects/vela-mc-blos/images'
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CUSTOM_DIR = os.path.join(SCRIPT_DIR, 'custom')
DEFAULT_MODIFIER = (
    os.path.join(CUSTOM_DIR, 'vela_config_modifier.ini')
    if os.path.isfile(os.path.join(CUSTOM_DIR, 'vela_config_modifier.ini'))
    else os.path.join(SCRIPT_DIR, 'vela_config_modifier.ini')
)
VERIFY_SCRIPT = os.path.join(SCRIPT_DIR, 'verify_calibration.py')
VERIFY_IMAGING_SCRIPT = os.path.join(SCRIPT_DIR, 'verify_imaging.py')
LINMOS_SCRIPT = os.path.join(SCRIPT_DIR, 'run_linmos.py')
PIPELINE_CMD = 'processMeerKAT.py'


def parse_args():
    parser = argparse.ArgumentParser(
        description='Vela MC-BLOS pipeline wrapper — calibration, verification, and imaging')
    parser.add_argument('-i', '--id', dest='obs_id', required=True,
                        help='Observation ID (e.g. 1770051982)')
    parser.add_argument('--setup-only', action='store_true',
                        help='Set up config and scripts but do not submit jobs')
    parser.add_argument('--skip-imaging', action='store_true',
                        help='Submit calibration + verification only, skip imaging')
    parser.add_argument('--force', action='store_true',
                        help='Overwrite existing working directory')
    parser.add_argument('--config', dest='modifier', default=DEFAULT_MODIFIER,
                        help='Path to config modifier file (default: vela_config_modifier.ini)')
    return parser.parse_args()


def find_raw_ms(obs_id):
    """Find the raw MS for the given observation ID."""
    obs_dir = os.path.join(RAW_BASE, obs_id)
    if not os.path.isdir(obs_dir):
        print("ERROR: raw observation directory not found: {}".format(obs_dir))
        sys.exit(1)

    ms_files = glob.glob(os.path.join(obs_dir, '*.ms'))
    if not ms_files:
        print("ERROR: no .ms file found in {}".format(obs_dir))
        sys.exit(1)

    if len(ms_files) > 1:
        ms_files.sort()
        print("Multiple MS files found in {}:".format(obs_dir))
        for idx, ms in enumerate(ms_files):
            print("  [{}] {}".format(idx, os.path.basename(ms)))
        print("Select a file (0-{}) [default: 0, timeout 60s]: ".format(len(ms_files) - 1),
              end='', flush=True)
        import select as _select
        ready, _, _ = _select.select([sys.stdin], [], [], 60)
        if ready:
            choice = sys.stdin.readline().strip()
            if choice.isdigit() and 0 <= int(choice) < len(ms_files):
                return ms_files[int(choice)]
            elif choice == '':
                print("No input, using default: {}".format(os.path.basename(ms_files[0])))
            else:
                print("Invalid choice '{}', using default: {}".format(
                    choice, os.path.basename(ms_files[0])))
        else:
            print("\nTimed out, using default: {}".format(os.path.basename(ms_files[0])))

    return ms_files[0]


def read_modifier(modifier_path):
    """Read the modifier config file.

    Returns a dict of section -> {key: value_string} for pipeline config
    sections, and the [sbatch_overrides] dict separately.
    """
    config = configparser.ConfigParser(allow_no_value=True)
    config.read(modifier_path)

    sections = {}
    sbatch_overrides = {}

    for section in config.sections():
        if section == 'sbatch_overrides':
            for key in config.options(section):
                sbatch_overrides[key] = config.get(section, key)
        else:
            sections[section] = {}
            for key in config.options(section):
                sections[section][key] = config.get(section, key)

    return sections, sbatch_overrides


def apply_modifier_to_config(config_file, modifier_sections):
    """Apply modifier overrides to a pipeline config file.

    Uses the pipeline's config_parser format (configparser with ast.literal_eval).
    """
    try:
        import config_parser as cp
        for section, params in modifier_sections.items():
            # Convert string values to Python literals for the pipeline format
            typed_params = {}
            for key, val in params.items():
                try:
                    parsed = ast.literal_eval(val)
                except (ValueError, SyntaxError):
                    parsed = val
                # Strings must stay quoted so overwrite_config writes them
                # as valid Python literals (e.g. '8s' not 8s)
                if isinstance(parsed, str):
                    typed_params[key] = "'{}'".format(parsed)
                else:
                    typed_params[key] = parsed
            cp.overwrite_config(config_file, conf_dict=typed_params, conf_sec=section)
        print("Applied modifier overrides to {}".format(config_file))
    except ImportError:
        # Fallback: direct configparser manipulation
        print("Warning: pipeline config_parser not available, using direct configparser")
        config = configparser.ConfigParser(allow_no_value=True)
        config.read(config_file)
        for section, params in modifier_sections.items():
            if not config.has_section(section):
                config.add_section(section)
            for key, val in params.items():
                config.set(section, key, val)
        with open(config_file, 'w') as f:
            config.write(f)
        print("Applied modifier overrides to {} (fallback mode)".format(config_file))


def get_config_scripts(config_file):
    """Extract all script names referenced in a pipeline config file."""
    config = configparser.ConfigParser(allow_no_value=True)
    config.read(config_file)
    script_names = set()
    for key in ('scripts', 'precal_scripts', 'postcal_scripts'):
        if config.has_option('slurm', key):
            raw = config.get('slurm', key)
            try:
                entries = ast.literal_eval(raw)
                for entry in entries:
                    if isinstance(entry, (list, tuple)) and len(entry) > 0:
                        script_names.add(entry[0])
                    elif isinstance(entry, str):
                        script_names.add(entry)
            except (ValueError, SyntaxError):
                pass
    return script_names


def deploy_custom_scripts(custom_dir, work_dir, config_file):
    """Copy custom .py scripts from custom_dir to work_dir.

    Warns about scripts not referenced in the pipeline config.
    """
    if not os.path.isdir(custom_dir):
        return
    import shutil
    custom_scripts = [f for f in os.listdir(custom_dir) if f.endswith('.py')]
    if not custom_scripts:
        return

    config_scripts = get_config_scripts(config_file)

    for script in custom_scripts:
        src = os.path.join(custom_dir, script)
        dst = os.path.join(work_dir, script)
        shutil.copy2(src, dst)
        print("Copied custom script: {}".format(script))

    unmatched = [s for s in custom_scripts if s not in config_scripts]
    if unmatched:
        print("Warning: custom scripts not referenced in config: {}".format(
            ', '.join(unmatched)))


def run_processMeerKAT(args_str):
    """Run processMeerKAT.py with the given arguments."""
    cmd = 'source /idia/software/pipelines/master/setup.sh && {} {}'.format(
        PIPELINE_CMD, args_str)
    print("Running: {}".format(cmd))
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                            executable='/bin/bash')
    if result.returncode != 0:
        print("STDOUT: {}".format(result.stdout))
        print("STDERR: {}".format(result.stderr))
        print("ERROR: processMeerKAT.py failed with return code {}".format(result.returncode))
        sys.exit(1)
    return result.stdout


def submit_pipeline_sh():
    """Run ./submit_pipeline.sh and return (all stdout, last job ID)."""
    print("Submitting calibration pipeline via submit_pipeline.sh")
    result = subprocess.run(['./submit_pipeline.sh'], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print("STDERR: {}".format(result.stderr))
        print("ERROR: submit_pipeline.sh failed")
        sys.exit(1)

    last_job = get_last_job_id(result.stdout)
    if last_job is None:
        print("ERROR: could not parse any job IDs from submit_pipeline.sh output")
        sys.exit(1)

    print("Last calibration job ID: {}".format(last_job))
    return result.stdout, last_job


def get_target_fields(config_file):
    """Read target field names from a pipeline config file.

    Returns a list of field name strings, e.g. ['Vela_00', 'Vela_01', ...].
    """
    try:
        import config_parser as cp
        config = cp.get_config(config_file)
        raw = config['fields']['targetfields']
    except (ImportError, KeyError):
        config = configparser.ConfigParser(allow_no_value=True)
        config.read(config_file)
        raw = config.get('fields', 'targetfields')

    # Strip surrounding quotes if present
    raw = raw.strip().strip("'\"")
    fields = [f.strip() for f in raw.split(',') if f.strip()]
    return fields


def setup_imaging_config(imaging_dir, modifier_sections, ms_path, target_field=None):
    """Create an imaging config with selfcal + science_image postcal scripts.

    If target_field is specified, restricts imaging to that single field.
    Returns the path to the imaging config file.
    """

    imaging_config = os.path.join(imaging_dir, 'imaging_config.txt')

    # Build config from the calibrated data
    run_processMeerKAT('-B -M {} -C {} -P'.format(ms_path, imaging_config))

    # Set postcal_scripts for imaging
    postcal = [
        "('selfcal_part1.py', True, '')",
        "('selfcal_part2.py', True, '')",
        "('science_image.py', True, '')",
    ]
    postcal_str = '[{}]'.format(', '.join(postcal))

    # Apply selfcal and imaging modifier sections
    imaging_modifiers = {}
    for section in ['selfcal', 'imaging', 'slurm']:
        if section in modifier_sections:
            imaging_modifiers[section] = modifier_sections[section]

    # Also set postcal_scripts and submit=False in slurm section
    if 'slurm' not in imaging_modifiers:
        imaging_modifiers['slurm'] = {}
    imaging_modifiers['slurm']['postcal_scripts'] = postcal_str
    imaging_modifiers['slurm']['submit'] = 'False'

    # Restrict to a single target field if specified
    if target_field is not None:
        if 'fields' not in imaging_modifiers:
            imaging_modifiers['fields'] = {}
        imaging_modifiers['fields']['targetfields'] = "'{}'".format(target_field)

    apply_modifier_to_config(imaging_config, imaging_modifiers)

    return imaging_config


def main():
    args = parse_args()
    obs_id = args.obs_id

    print("=" * 60)
    print("Vela MC-BLOS Pipeline Wrapper")
    print("Obs ID: {}".format(obs_id))
    print("=" * 60)

    # Step 1: Find raw MS
    ms_path = find_raw_ms(obs_id)
    print("Found MS: {}".format(ms_path))

    # Step 2: Create working directory
    work_dir = os.path.join(SCRATCH_BASE, obs_id)
    if os.path.exists(work_dir):
        if args.force:
            print("Warning: --force set, working in existing directory {}".format(work_dir))
        else:
            print("ERROR: working directory already exists: {}".format(work_dir))
            print("Use --force to override, or manually remove it.")
            sys.exit(1)
    else:
        os.makedirs(work_dir, mode=0o775)
        print("Created working directory: {}".format(work_dir))

    os.chdir(work_dir)
    print("Working in: {}".format(os.getcwd()))

    # Read modifier config
    modifier_sections, sbatch_overrides = read_modifier(args.modifier)
    print("Read modifier config: {}".format(args.modifier))

    # Step 3: Build config (auto-detect fields, enable pol)
    config_file = os.path.join(work_dir, 'default_config.txt')
    run_processMeerKAT('-B -M {} -C {} -P'.format(ms_path, config_file))
    print("Built default config: {}".format(config_file))

    # Step 4: Apply modifier overrides (excluding sbatch_overrides and imaging sections)
    cal_sections = {k: v for k, v in modifier_sections.items()
                    if k not in ('selfcal', 'imaging')}
    apply_modifier_to_config(config_file, cal_sections)

    # Step 4b: Deploy custom scripts to work dir (before -R picks them up)
    deploy_custom_scripts(CUSTOM_DIR, work_dir, config_file)

    # Step 5: Generate scripts (does NOT submit since submit=False in config)
    # Use relative path — processMeerKAT's spw_split does string concatenation
    # with the config path, so an absolute path breaks SPW directory creation.
    run_processMeerKAT('-R -C {}'.format(os.path.basename(config_file)))
    print("Generated sbatch scripts")

    # Step 6: Apply sbatch overrides
    if sbatch_overrides:
        apply_sbatch_overrides(sbatch_overrides)
        print("Applied sbatch overrides")

    if args.setup_only:
        print("\n--setup-only: stopping after script generation.")
        print("Inspect {} and sbatch files, then submit manually.".format(config_file))
        sys.exit(0)

    # Step 7: Submit calibration
    submit_stdout, cal_final_job_id = submit_pipeline_sh()

    # Step 8: Submit verification job
    account = modifier_sections.get('slurm', {}).get('account', 'b223-vela-mc-blos-ag')
    verify_sbatch = os.path.join(work_dir, 'verify_cal.sbatch')
    verify_cmd = 'python3 {} --obs-id {} --cal-dir {}'.format(VERIFY_SCRIPT, obs_id, work_dir)
    write_sbatch_script(
        verify_sbatch,
        job_name='verify_cal_{}'.format(obs_id),
        command=verify_cmd,
        account=account,
        time='00:10:00',
        mem='4GB',
    )
    verify_job_id = submit_sbatch(verify_sbatch, dependency=cal_final_job_id, dep_type='afterany')
    if verify_job_id is None:
        print("ERROR: failed to submit verification job")
        sys.exit(1)
    print("Verification job submitted: {} (afterany:{})".format(verify_job_id, cal_final_job_id))

    if args.skip_imaging:
        print("\n--skip-imaging: stopping after verification submission.")
        print("Monitor with: squeue -u $USER")
        sys.exit(0)

    # Step 9: Submit imaging jobs — one per target field
    target_fields = get_target_fields(config_file)
    print("Target fields ({}): {}".format(len(target_fields), ', '.join(target_fields)))

    all_last_imaging_jobs = []

    for field in target_fields:
        print("\n--- Imaging: {} ---".format(field))

        # Create a per-field subdirectory for imaging scripts
        field_imaging_dir = os.path.join(work_dir, 'imaging_{}'.format(field))
        if not os.path.exists(field_imaging_dir):
            os.makedirs(field_imaging_dir, mode=0o775)
        os.chdir(field_imaging_dir)

        # Build imaging config restricted to this field
        imaging_config = setup_imaging_config(
            field_imaging_dir, modifier_sections, ms_path, target_field=field)

        # Generate imaging scripts
        run_processMeerKAT('-R -C {}'.format(imaging_config))
        print("Generated imaging sbatch scripts for {}".format(field))

        # Modify submit_pipeline.sh: first sbatch depends on verify job
        submit_script = os.path.join(field_imaging_dir, 'submit_pipeline.sh')
        with open(submit_script) as f:
            lines = f.readlines()

        modified = False
        new_lines = []
        for line in lines:
            if not modified and 'sbatch' in line and '#' not in line.lstrip()[:1]:
                line = line.replace(
                    'sbatch ',
                    'sbatch --dependency=afterok:{} '.format(verify_job_id), 1)
                modified = True
            new_lines.append(line)

        with open(submit_script, 'w') as f:
            f.writelines(new_lines)

        # Submit this field's imaging pipeline
        print("Submitting imaging for {} (first job afterok:{})".format(field, verify_job_id))
        img_result = subprocess.run(
            ['./submit_pipeline.sh'], capture_output=True, text=True,
            cwd=field_imaging_dir)
        print(img_result.stdout)
        if img_result.returncode != 0:
            print("STDERR: {}".format(img_result.stderr))
            print("ERROR: imaging submit_pipeline.sh failed for {}".format(field))
            sys.exit(1)

        last_job = get_last_job_id(img_result.stdout)
        if last_job:
            print("Last imaging job for {}: {}".format(field, last_job))
            all_last_imaging_jobs.append(last_job)
        else:
            print("Warning: could not parse imaging job IDs for {}".format(field))

    # Step 10: Submit imaging verification job (afterany:all imaging jobs)
    verify_img_job_id = None
    if all_last_imaging_jobs:
        imaging_dep = ':'.join(all_last_imaging_jobs)

        verify_img_sbatch = os.path.join(work_dir, 'verify_imaging.sbatch')
        verify_img_cmd = 'python3 {} --obs-id {} --work-dir {}'.format(
            VERIFY_IMAGING_SCRIPT, obs_id, work_dir)
        write_sbatch_script(
            verify_img_sbatch,
            job_name='verify_img_{}'.format(obs_id),
            command=verify_img_cmd,
            account=account,
            time='00:10:00',
            mem='4GB',
        )
        verify_img_job_id = submit_sbatch(
            verify_img_sbatch, dependency=imaging_dep, dep_type='afterany')
        if verify_img_job_id is None:
            print("ERROR: failed to submit imaging verification job")
            sys.exit(1)
        print("Imaging verification job submitted: {} (afterany:{})".format(
            verify_img_job_id, imaging_dep))

    # Step 11: Submit linmos mosaicking job (afterok:verify_imaging)
    linmos_job_id = None
    if verify_img_job_id:
        container = modifier_sections.get('slurm', {}).get(
            'container', '/idia/software/containers/casa-6.5.0-modular.sif')
        linmos_cmd = 'singularity exec {} python3 {} --work-dir {}'.format(
            container, LINMOS_SCRIPT, work_dir)
        linmos_sbatch = os.path.join(work_dir, 'linmos.sbatch')
        write_sbatch_script(
            linmos_sbatch,
            job_name='linmos_{}'.format(obs_id),
            command=linmos_cmd,
            account=account,
            time='02:00:00',
            mem='64GB',
        )
        linmos_job_id = submit_sbatch(
            linmos_sbatch, dependency=verify_img_job_id, dep_type='afterok')
        if linmos_job_id:
            print("Linmos job submitted: {} (afterok:{})".format(
                linmos_job_id, verify_img_job_id))
        else:
            print("Warning: failed to submit linmos job")

    # Step 12: Submit image-move job (afterany:linmos or afterany:all imaging jobs)
    move_job_id = None
    if all_last_imaging_jobs:
        # Depend on linmos if it was submitted, otherwise on imaging jobs
        if linmos_job_id:
            move_dep = linmos_job_id
        else:
            move_dep = ':'.join(all_last_imaging_jobs)

        images_dest = os.path.join(IMAGES_BASE, obs_id)
        # Collect images from all per-field imaging directories + mosaic
        copy_lines = [
            'set -e',
            'mkdir -p {}'.format(images_dest),
        ]
        for field in target_fields:
            field_dir = os.path.join(work_dir, 'imaging_{}'.format(field))
            copy_lines.extend([
                '# Copy images for {}'.format(field),
                'for img in {}/*image*; do'.format(field_dir),
                '    if [ -e "$img" ]; then',
                '        cp -r "$img" {}/'.format(images_dest),
                '    fi',
                'done',
                'for fits in {}/*.fits; do'.format(field_dir),
                '    if [ -e "$fits" ]; then',
                '        cp -r "$fits" {}/'.format(images_dest),
                '    fi',
                'done',
            ])
        # Copy mosaic products
        copy_lines.extend([
            '# Copy mosaic products',
            'for mosaic in {}/mosaic.*; do'.format(work_dir),
            '    if [ -e "$mosaic" ]; then',
            '        cp -r "$mosaic" {}/'.format(images_dest),
            '    fi',
            'done',
        ])
        copy_lines.append('echo "Images moved to {}"'.format(images_dest))
        move_cmd = '\n'.join(copy_lines)

        move_sbatch = os.path.join(work_dir, 'move_images.sbatch')
        write_sbatch_script(
            move_sbatch,
            job_name='move_images_{}'.format(obs_id),
            command=move_cmd,
            account=account,
            time='01:00:00',
            mem='8GB',
        )
        move_job_id = submit_sbatch(
            move_sbatch, dependency=move_dep, dep_type='afterany')
        if move_job_id:
            print("Image-move job submitted: {} (afterany:{})".format(
                move_job_id, move_dep))
        else:
            print("Warning: failed to submit image-move job")

    # Summary
    print("\n" + "=" * 60)
    print("Pipeline submitted successfully!")
    print("SLURM dependency chain:")
    print("  Calibration (last job: {})".format(cal_final_job_id))
    print("    -> Verify calibration (job: {})".format(verify_job_id))
    if not args.skip_imaging and all_last_imaging_jobs:
        print("    -> Imaging ({} fields, {} jobs):".format(
            len(target_fields), len(all_last_imaging_jobs)))
        for field, job in zip(target_fields, all_last_imaging_jobs):
            print("         {} (last job: {})".format(field, job))
        if verify_img_job_id:
            print("    -> Verify imaging (job: {})".format(verify_img_job_id))
        if linmos_job_id:
            print("    -> Linmos mosaic (job: {})".format(linmos_job_id))
        if move_job_id:
            print("    -> Move images (job: {})".format(move_job_id))
    print("\nMonitor with: squeue -u $USER")
    print("Working directory: {}".format(work_dir))
    print("=" * 60)


if __name__ == '__main__':
    main()
