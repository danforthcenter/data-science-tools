#!/usr/bin/env python

import os
import shutil
import argparse
import json
import hashlib
import mimetypes


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

    # Read the configuration file
    config_file = open(args.config, 'r')
    # Load the JSON configuration data
    config = json.load(config_file)

    # Make sure the acquisition directory is valid
    # Does the path exist?
    if not os.path.exists(config["data_acquisition_path"]):
        raise IOError("The data acquisition directory {0} does not exist.".format(config["data_acquisition_path"]))

    # We expect the acqusition directory to have three subdirectories (kinect2, trajectories, and vnir)
    contents = os.listdir(config["data_acquisition_path"])
    if "kinect2" not in contents and "vnir" not in contents:
        raise IOError("The data acquisition directory {0} should contain the kinect2 "
                      "and vnir subdirectories.".format(config["data_acquisition_path"]))

    # Make sure each project directory exists, otherwise make them
    for project in config["project_data_paths"].keys():
        # Does the project path exist?
        if not os.path.exists(config["project_data_paths"][project]):
            os.mkdir(config["project_data_paths"][project])
            os.mkdir(os.path.join(config["project_data_paths"][project], "kinect2"))
            os.mkdir(os.path.join(config["project_data_paths"][project], "vnir"))
        else:
            # Make sure the subdirectories exist
            if not os.path.exists(os.path.join(config["project_data_paths"][project], "kinect2")):
                os.mkdir(os.path.join(config["project_data_paths"][project], "kinect2"))
            if not os.path.exists(os.path.join(config["project_data_paths"][project], "vnir")):
                os.mkdir(os.path.join(config["project_data_paths"][project], "vnir"))

    # Walk through the vnir directory to look for JSON files
    for (dirpath, dirnames, filenames) in os.walk(os.path.join(config["data_acquisition_path"], "vnir")):
        # Loop over the files only
        for filename in filenames:
            # Look for JSON files
            if "application/json" in mimetypes.guess_type(filename):
                # If the file is JSON, open it
                with open(os.path.join(dirpath, filename), "r") as f:
                    # Load the contents of the file using the JSON importer
                    metadata = json.load(f)
                    # Find the sampleId field
                    sample_id = metadata["lemnatec_measurement_metadata"]["user_given_metadata"]["sampleId"]
                    # Split the sampleId on underscores
                    info = sample_id.split("_")
                    # The project ID is the first item in the list
                    project_id = info[0]
                    # Check if the project ID is in the configuration file
                    if project_id in config["project_data_paths"]:
                        # The project path is the destination directory
                        project_path = config["project_data_paths"][project_id]
                        # Find the two parent directories in the source directory
                        fs, scandir = os.path.split(dirpath)
                        vnir, datedir = os.path.split(fs)
                        # Make the date directory in the destination directory if needed
                        if not os.path.exists(os.path.join(project_path, "vnir", datedir)):
                            os.mkdir(os.path.join(project_path, "vnir", datedir))
                        if not os.path.exists(os.path.join(project_path, "kinect2", datedir)):
                            os.mkdir(os.path.join(project_path, "kinect2", datedir))
                        # Make the scan directory in the destination directory if needed
                        if not os.path.exists(os.path.join(project_path, "vnir", datedir, scandir)):
                            os.mkdir(os.path.join(project_path, "vnir", datedir, scandir))
                        if not os.path.exists(os.path.join(project_path, "kinect2", datedir, scandir)):
                            os.mkdir(os.path.join(project_path, "kinect2", datedir, scandir))
                        # Get the list of files in the scan directory
                        vnir_files = os.listdir(dirpath)
                        for vnir_file in vnir_files:
                            # Get source checksum
                            src_checksum = checksum(os.path.join(dirpath, vnir_file))
                            # Copy the file to the destination directory
                            shutil.copy2(os.path.join(dirpath, vnir_file),
                                         os.path.join(project_path, "vnir", datedir, scandir))
                            # Get destination checksum
                            dest_checksum = checksum(os.path.join(dirpath, vnir_file))
                            # Make sure the source and destination checksums match
                            if src_checksum != dest_checksum:
                                raise IOError("The file {0} was not copied correctly, "
                                              "stopping transfers.".format(os.path.join(dirpath, vnir_file)))
                        # Get the list of corresponding kinect files
                        kinect_dir = os.path.join(config["data_acquisition_path"], "kinect2", datedir, scandir)
                        kinect_files = os.listdir(kinect_dir)
                        for kinect_file in kinect_files:
                            # Get source checksum
                            src_checksum = checksum(os.path.join(kinect_dir, kinect_file))
                            # Copy the file to the destination directory
                            shutil.copy2(os.path.join(kinect_dir, kinect_file),
                                         os.path.join(project_path, "kinect2", datedir, scandir))
                            # Get destination checksum
                            dest_checksum = checksum(os.path.join(kinect_dir, kinect_file))
                            # Make sure the source and destination checksums match
                            if src_checksum != dest_checksum:
                                raise IOError("The file {0} was not copied correctly, "
                                              "stopping transfers.".format(os.path.join(kinect_dir, kinect_file)))


def checksum(filename):
    data = open(filename, "rb")
    md5sum = hashlib.md5()
    # Loop over chunks of the file so we don't read it all into memory
    for chunk in iter(lambda: data.read(4096), b""):
        # Update the checksum with each chunk
        md5sum.update(chunk)

    return md5sum.hexdigest()


if __name__ == '__main__':
    main()
