#!/usr/bin/env python3
"""Linear mosaicking of per-field Vela MC-BLOS images.

Combines per-field *.image products into a single mosaic using
casatasks.linearmosaic, with primary beam (*.pb) weighting. Exports
the result to FITS.

Designed to run inside the CASA 6.5 modular container via:
    singularity exec <container> python3 run_linmos.py --work-dir <path>

Usage:
    python3 run_linmos.py --work-dir <path> [--output <path>]
"""

import argparse
import configparser
import ast
import glob
import os
import sys


def parse_args():
    parser = argparse.ArgumentParser(description='Linear mosaic of Vela per-field images')
    parser.add_argument('--work-dir', required=True,
                        help='Pipeline working directory containing imaging_<field>/ subdirs')
    parser.add_argument('--output', default=None,
                        help='Output basename (default: <work_dir>/mosaic)')
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


def find_image_products(work_dir, target_fields):
    """Find *.image and *.pb products across all per-field imaging directories.

    Returns (images, pbs) where each is a list of paths, matched by index.
    """
    images = []
    pbs = []

    for field in target_fields:
        field_dir = os.path.join(work_dir, 'imaging_{}'.format(field))
        if not os.path.isdir(field_dir):
            print("Warning: imaging directory missing for {}, skipping".format(field))
            continue

        field_images = sorted(glob.glob(os.path.join(field_dir, '*.image')))
        field_pbs = sorted(glob.glob(os.path.join(field_dir, '*.pb')))

        if not field_images:
            print("Warning: no *.image found for {}, skipping".format(field))
            continue
        if not field_pbs:
            print("Warning: no *.pb found for {}, skipping".format(field))
            continue

        # Use the first image/pb pair (typically one science image per field)
        images.append(field_images[0])
        pbs.append(field_pbs[0])
        print("Field {}: image={}, pb={}".format(
            field, os.path.basename(field_images[0]), os.path.basename(field_pbs[0])))

    return images, pbs


def main():
    args = parse_args()
    work_dir = os.path.abspath(args.work_dir)
    output_base = args.output if args.output else os.path.join(work_dir, 'mosaic')

    print("=" * 60)
    print("Vela Linear Mosaic")
    print("Work dir: {}".format(work_dir))
    print("Output:   {}".format(output_base))
    print("=" * 60)

    # Import CASA tasks
    try:
        from casatasks import linearmosaic, exportfits
    except ImportError:
        print("ERROR: casatasks not available. Run inside the CASA container:")
        print("  singularity exec <container> python3 run_linmos.py --work-dir <path>")
        sys.exit(1)

    # Read target fields
    config_file = os.path.join(work_dir, 'default_config.txt')
    if not os.path.exists(config_file):
        print("ERROR: default_config.txt not found in {}".format(work_dir))
        sys.exit(1)

    target_fields = get_target_fields(config_file)
    if not target_fields:
        print("ERROR: no target fields found in config")
        sys.exit(1)
    print("Target fields ({}): {}".format(len(target_fields), ', '.join(target_fields)))

    # Find image products
    images, pbs = find_image_products(work_dir, target_fields)

    if len(images) < 2:
        print("ERROR: need at least 2 fields for mosaicking, found {}".format(len(images)))
        sys.exit(1)

    # Run linear mosaic
    output_image = '{}.image'.format(output_base)
    print("\nRunning linearmosaic with {} fields...".format(len(images)))
    print("  images: {}".format([os.path.basename(i) for i in images]))
    print("  pbs:    {}".format([os.path.basename(p) for p in pbs]))
    print("  output: {}".format(output_image))

    linearmosaic(
        images=images,
        weightimages=pbs,
        output=output_image,
    )
    print("Linear mosaic complete: {}".format(output_image))

    # Export to FITS
    output_fits = '{}.fits'.format(output_base)
    print("Exporting to FITS: {}".format(output_fits))
    exportfits(
        imagename=output_image,
        fitsimage=output_fits,
        overwrite=True,
    )
    print("FITS export complete: {}".format(output_fits))

    print("\n" + "=" * 60)
    print("Mosaic products:")
    print("  {}".format(output_image))
    print("  {}".format(output_fits))
    print("=" * 60)


if __name__ == '__main__':
    main()
