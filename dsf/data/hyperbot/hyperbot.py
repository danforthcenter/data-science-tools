"""Hyperbot automation functions"""
import os
import json
import mimetypes
import shutil
from dsf.data.utils import checksum
from dsf.data.utils import compare_checksums
from dsf.data.utils import validate_file_transfer


def validate_config(config):
    """Validate configuration file data.

    Keyword arguments:
    config -- configuration dictionary data

    :param config: dict
    """
    # Make sure the acquisition directory is valid
    # Does the path exist?
    if not os.path.exists(config["data_acquisition_path"]):
        raise IOError("The data acquisition directory {0} does not exist.".format(config["data_acquisition_path"]))

    # We expect the acqusition directory to have three subdirectories (kinect2, trajectories, and vnir)
    contents = os.listdir(config["data_acquisition_path"])
    for sensor in config["sensors"]:
        if sensor not in contents:
            raise IOError("The data acquisition directory {0} should contain "
                          "the sensor subdirectory {1}".format(config["data_acquisition_path"], sensor))


def find_scans(config):
    """Find scan directories.

    Keyword arguments:
    config -- configuration dictionary data

    :param config: dict
    :return scans: dict
    """
    scans = {"skipped": []}
    # Walk through each sensor directory in the data acquisition directory to look for JSON files
    for sensor in config["sensors"]:
        scans[sensor] = {}
        for (dirpath, dirnames, filenames) in os.walk(os.path.join(config["data_acquisition_path"], sensor)):
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
                        # Find the two parent directories in the source directory
                        fs, scandir = os.path.split(dirpath)
                        sensor_dir, datedir = os.path.split(fs)
                        # Split the sampleId on underscores
                        info = sample_id.split("_")
                        # The project ID is the first item in the list
                        project_id = info[0]
                        if project_id in config["project_data_paths"]:
                            scans[sensor][scandir] = {"sample_id": sample_id, "project_id": project_id, "date": datedir}
                        else:
                            scans["skipped"].append(dirpath)
    return scans


def init_scan_dirs(scans, config):
    """Initialize scan directories in the target project directories.

    Keyword arguments:
    scans -- dictionary of scans found in the data acquisition directory
    config -- configuration dictionary data

    :param scans: dict
    :param config: dict
    :return tasks: list
    """
    tasks = []
    # Loop over sensors
    for sensor in scans.keys():
        if sensor != "skipped":
            # Loop over sensor scans
            for scan in scans[sensor].keys():
                # Project directory
                project_dir = config["project_data_paths"][scans[sensor][scan]["project_id"]]
                # Make sure the project directory exists, otherwise make it
                if not os.path.exists(project_dir):
                    os.mkdir(project_dir)
                # Make the date directory in the destination directory if needed
                # datedir = os.path.join(project_dir, sensor, scans[sensor][scan]["date"])
                # if not os.path.exists(datedir):
                #     os.mkdir(datedir)
                # Make the scan directory in the destination directory if needed
                target_dir = os.path.join(project_dir, scans[sensor][scan]["sample_id"] + "_" + scan)
                if not os.path.exists(target_dir):
                    os.mkdir(target_dir)
                # Populate task list
                source_dir = os.path.join(config["data_acquisition_path"], sensor, scans[sensor][scan]["date"], scan)
                tasks.append({"source_dir": source_dir, "target_dir": target_dir, "sensor": sensor})
    return tasks


def move_scan_files(tasks):
    """Move scan files from source to target directory securely.

    Keyword arguments:
    tasks -- list of data moving tasks (source and target directories

    :param tasks: list
    """
    for task in tasks:
        # List of files in source directory
        file_list = os.listdir(task["source_dir"])
        for source_file in file_list:
            target_file = task["sensor"] + "_" + source_file
            # Get source checksum
            src_checksum = checksum(os.path.join(task["source_dir"], source_file))
            # Copy the file to the destination directory
            shutil.move(os.path.join(task["source_dir"], source_file),
                         os.path.join(task["target_dir"], target_file))
            # Get destination checksum
            dest_checksum = checksum(filename=os.path.join(task["target_dir"], target_file))
            # Make sure the source and destination checksums match
            validate_file_transfer(filename=os.path.join(task["source_dir"], source_file),
                                   file_transfer_status=compare_checksums(chksum1=src_checksum,
                                                                          chksum2=dest_checksum))

def delete_source_scan_files(scans, config):
    """Delete source scan files.

    Keyword arguments:
    scans -- dictionary of scans found in the data acquisition directory
    config -- configuration dictionary data

    :param scans: dict
    :param config: dict
    """
    # Loop over sensors
    for sensor in scans.keys():
        if sensor != "skipped":
            # Loop over sensor scans
            for scan in scans[sensor].keys():
                source_dir = os.path.join(config["data_acquisition_path"], sensor, scans[sensor][scan]["date"], scan)
                shutil.rmtree(source_dir)
