"""Phenomation utility functions."""
import json
import hashlib


def checksum(filename):
    """Get file checksum.

    Keyword arguments:
    filename -- input filename

    :param filename: str
    :return md5sum: str
    """
    data = open(filename, "rb")
    md5sum = hashlib.md5()
    # Loop over chunks of the file so we don't read it all into memory
    for chunk in iter(lambda: data.read(4096), b""):
        # Update the checksum with each chunk
        md5sum.update(chunk)

    return md5sum.hexdigest()


def compare_checksums(chksum1, chksum2):
    """Compare two checksums for equality.

    Keyword arguments:
    chksum1 -- first checksum value
    chksum2 -- second checksum value

    :param chksum1: str
    :param chksum2: str
    :return file_copy_status: bool
    """
    file_copy_status = chksum1 == chksum2
    return file_copy_status


def validate_file_transfer(filename, file_transfer_status):
    """Validate file transfer.

    Keyword arguments:
    filename -- name of file being copied
    file_transfer_status -- true (success) or false (failed)

    :param filename: str
    :param file_transfer_status: bool
    """
    if not file_transfer_status:
        raise IOError("Copying file {0} failed, stopping transfers!".format(filename))
    return True


def load_config(json_file):
    """Load dsf configuration file.

    Keyword arguments:
    json_file -- configuration file in JSON format

    :param json_file: str
    :return config: dict
    """
    # Read the configuration file
    with open(json_file, "r") as f:
        # Load the JSON configuration data
        config = json.load(f)
        return config
