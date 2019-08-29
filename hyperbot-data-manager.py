#!/usr/bin/env python

import os
import shutil
import argparse
import json
import mimetypes
import dsf


def options():
    # Create argparse parser object
    parser = argparse.ArgumentParser(description="Data management program for the hyperbot.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # Add argument for an input configuration file
    parser.add_argument("-c", "--config", help="Configuration file (JSON format).", required=True)
    # Parse command-line arguments
    args = parser.parse_args()

    # Return the argparse object
    return args


def main():
    # Parse command-line arguments
    args = options()

    # Load the JSON configuration data
    config = dsf.data.utils.load_config(json_file=args.config)

    # Validate configuration data
    dsf.data.hyperbot.validate_config(config=config)

    # Find scan directories
    scans = dsf.data.hyperbot.find_scans(config=config)

    # Initialize scan directories if needed
    tasks = dsf.data.hyperbot.init_scan_dirs(scans=scans, config=config)

    # Copy files
    dsf.data.hyperbot.move_scan_files(tasks=tasks)

    # Delete source files
    dsf.data.hyperbot.delete_source_scan_files(scans=scans, config=config)


if __name__ == '__main__':
    main()
