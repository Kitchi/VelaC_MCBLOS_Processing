# run_vela.py — Vela MC-BLOS Pipeline Wrapper

Automates the IDIA MeerKAT calibration and imaging pipeline for the
Vela MC-BLOS project. A single command orchestrates directory setup,
calibration, post-calibration verification, per-field imaging, and
image archival, all linked via SLURM job dependencies.

## Quick start

```bash
# Full pipeline — calibration, verification, imaging, and archival
./run_vela.py -i 1770051982
```

## Usage modes

### Full pipeline (default)

```bash
./run_vela.py -i <obs_id>
```

Runs all 12 steps end-to-end:

| Step | What happens |
|------|-------------|
| 1 | Locate the raw MS under `/idia/projects/vela-mc-blos/raw/SCI-20251102-MT-01/<obs_id>/` |
| 2 | Create a working directory at `/scratch3/projects/vela-mc-blos/<obs_id>/` |
| 3 | Build pipeline config via `processMeerKAT.py -B` |
| 4 | Apply calibration overrides from `vela_config_modifier.ini` |
| 5 | Generate sbatch scripts via `processMeerKAT.py -R` |
| 6 | Apply per-script SLURM resource overrides (`[sbatch_overrides]`) |
| 7 | Submit the calibration pipeline |
| 8 | Submit a calibration verification job (checks caltables and MMS files) |
| 9 | Submit imaging jobs — one independent job per target field, all in parallel |
| 10 | Submit an imaging verification job (checks for `*.image` and `*.pb` products) |
| 11 | Submit a linmos mosaicking job (combines per-field images into a single mosaic) |
| 12 | Submit an image-move job that copies per-field images + mosaic to `/idia/projects/vela-mc-blos/images/<obs_id>/` |

### Setup only (inspect before submitting)

```bash
./run_vela.py -i <obs_id> --setup-only
```

Runs steps 1–6 only. Creates the working directory, builds the config,
generates all sbatch scripts, and applies overrides — but does **not**
submit any jobs. Use this to inspect `default_config.txt` and the
generated sbatch files before committing to a run.

### Calibration + verification only (skip imaging)

```bash
./run_vela.py -i <obs_id> --skip-imaging
```

Runs steps 1–8. Submits calibration and the verification job, but does
not launch imaging. Useful when you want to check calibration quality
before proceeding.

### Reuse an existing working directory

```bash
./run_vela.py -i <obs_id> --force
```

By default the script exits if the working directory already exists.
`--force` allows it to continue in the existing directory, which is
useful for re-running after a partial failure.

### Use an alternate modifier config

```bash
./run_vela.py -i <obs_id> --config my_overrides.ini
```

Overrides the default `vela_config_modifier.ini` with a custom file.
The format is the same — see [Configuration](#configuration) below.

## SLURM dependency chain

```
Calibration (chained internally by submit_pipeline.sh)
    └── verify_cal (afterany:last_cal_job)
            └── imaging_Vela_00 (afterok:verify_cal)  ─┐
                imaging_Vela_01 (afterok:verify_cal)   │  all run
                imaging_Vela_02 (afterok:verify_cal)   ├─ in parallel
                ...                                    │
                imaging_Vela_18 (afterok:verify_cal)  ─┘
                        └── verify_imaging (afterany:all imaging jobs)
                                └── linmos (afterok:verify_imaging)
                                        └── move_images (afterany:linmos)
                                                → /idia/projects/vela-mc-blos/images/<obs_id>/
```

- **afterany** for verify_cal: runs even if calibration fails, so
  failures are logged.
- **afterok** for imaging: only runs if calibration verification passes.
- **afterany** for verify_imaging: runs after all imaging jobs finish
  (success or failure), to log results.
- **afterok** for linmos: only runs if imaging verification passes.
- **afterany** for move_images: runs after linmos finishes, copying
  whatever was produced (per-field images + mosaic).

Each target field is imaged independently in its own subdirectory
(`imaging_Vela_00/`, `imaging_Vela_01/`, etc.) with its own config and
sbatch scripts. The list of target fields is read automatically from
the `[fields] targetfields` entry in the pipeline config.

## Configuration

### vela_config_modifier.ini

Controls all tuneable parameters. Sections:

| Section | Purpose |
|---------|---------|
| `[crosscal]` | Calibration parameters (chanbin, refant, standard, etc.) |
| `[slurm]` | SLURM resources (nodes, mem, partition, account, container) |
| `[run]` | Pipeline run options (e.g. `dopol = True`) |
| `[sbatch_overrides]` | Per-script time/mem/ntasks overrides (format: `scriptname.param = value`) |
| `[selfcal]` | Self-calibration parameters (nloops, cell, robust, imsize, niter) |
| `[imaging]` | Final science imaging parameters (cell, robust, stokes, threshold) |

The `[crosscal]`, `[slurm]`, and `[run]` sections are applied to the
calibration config. The `[selfcal]`, `[imaging]`, and `[slurm]`
sections are applied to each per-field imaging config.

### sbatch_overrides format

```ini
[sbatch_overrides]
flag_round_1.mem = 64GB
flag_round_1.time = 2:00:00
setjy.time = 00:40:00
```

Each key is `<script_name>.<parameter>`. Supported parameters: `time`,
`mem`, `nodes`, `ntasks_per_node`, `cpus_per_task`. Overrides are
applied to both root-level and per-SPW sbatch files.

## Supporting scripts

| File | Purpose |
|------|---------|
| `slurm_utils.py` | Shared SLURM utilities (sbatch modification, job submission, script generation) |
| `verify_calibration.py` | Post-calibration checks (caltable completeness, MMS existence); logs to `calibration_log.csv` |
| `verify_imaging.py` | Post-imaging checks (`*.image` and `*.pb` existence per field); logs to `imaging_log.csv` |
| `run_linmos.py` | Linear mosaicking of per-field images via `casatasks.linearmosaic`; exports to FITS |
| `vela_config_modifier.ini` | Default parameter overrides (tested on obs 1770051982) |

## Monitoring

```bash
# Watch your jobs
squeue -u $USER

# Check calibration verification result
cat /scratch3/projects/vela-mc-blos/calibration_log.csv

# Check imaging verification result
cat /scratch3/projects/vela-mc-blos/imaging_log.csv

# Check a specific job's log
cat /scratch3/projects/vela-mc-blos/<obs_id>/verify_cal_<obs_id>-<jobid>.log
```

## Requirements

- IDIA cluster with SLURM
- `processMeerKAT.py` on `$PATH` (from `/idia/software/pipelines/master/processMeerKAT/`)
- Python 3 with standard library (no extra dependencies outside the pipeline)
