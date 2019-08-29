#!/usr/bin/env python

import os
import shutil
import json
from copy import deepcopy
import pytest
import dsf

TEST_TMPDIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".cache")
TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CONFIG_DATA = {
    "data_acquisition_path": os.path.join(TEST_DATA, "acquisition"),
    "sensors": ["vnir", "kinect2"],
    "project_data_paths": {
        "project": os.path.join(TEST_TMPDIR, "project")
    }
}
CONFIG_DAP_DNE = {
    "data_acquisition_path": os.path.join(TEST_DATA, "does-not-exist"),
    "sensors": ["vnir", "kinect2"],
    "project_data_paths": {
        "project": "project"
    }
}
CONFIG_DAP_CONTENT = {
    "data_acquisition_path": os.path.join(TEST_DATA),
    "sensors": ["vnir", "kinect2"],
    "project_data_paths": {
        "project": os.path.join(TEST_TMPDIR, "project")
    }
}

SCANS = {
    "skipped": [os.path.join(TEST_DATA, "acquisition", "vnir", "2019-08-08", "2019-08-08__18-38-21-380"),
                os.path.join(TEST_DATA, "acquisition", "kinect2", "2019-08-08", "2019-08-08__18-38-21-380")],
    "vnir": {
        "2019-08-08__16-38-21-380": {
            "sample_id": "project_test",
            "project_id": "project",
            "date": "2019-08-08"
        }
    },
    "kinect2": {
        "2019-08-08__16-38-21-380": {
            "sample_id": "project_test",
            "project_id": "project",
            "date": "2019-08-08"
        }
    }
}

TASKS = [{
    "source_dir": os.path.join(TEST_TMPDIR, "acquisition", "vnir", "2019-08-08", "2019-08-08__16-38-21-380"),
    "target_dir": os.path.join(TEST_TMPDIR, "project", "project_test_2019-08-08__16-38-21-380"),
    "sensor": "vnir"
}]

def setup_function():
    """Test setup function."""
    if not os.path.exists(TEST_TMPDIR):
        os.mkdir(TEST_TMPDIR)


def test_data_utils_read_config():
    # Create config file
    config_data = {
        "data_acquisition_path": os.path.join(TEST_DATA, "acquisition"),
        "sensors": ["vnir", "kinect2"],
        "project_data_paths": {
            "project": os.path.join(TEST_TMPDIR, "project")
        }
    }
    # Open config file
    with open(os.path.join(TEST_TMPDIR, "project.config"), "w") as f:
        # Save config data
        json.dump(config_data, f)
    # Read config data
    config = dsf.data.utils.load_config(json_file=os.path.join(TEST_TMPDIR, "project.config"))
    assert config["data_acquisition_path"] == os.path.join(TEST_DATA, "acquisition")


def test_data_utils_checksum():
    # File to calculate checksum on
    test_file = os.path.join(TEST_DATA, "acquisition", "vnir", "2019-08-08", "2019-08-08__16-38-21-380",
                             "26bb8db2-c4ff-4081-bca8-40b8be56ad77_data")
    chksum = dsf.data.utils.checksum(filename=test_file)
    assert chksum == "d41d8cd98f00b204e9800998ecf8427e"


@pytest.mark.parametrize("chksum1,chksum2,expected", [
    ("d41d8cd98f00b204e9800998ecf8427e", "d41d8cd98f00b204e9800998ecf8427e", True),
    ("d41d8cd98f00b204e9800998ecf8427e", "d41d8cd98f00b204e9800998ecf8427h", False)
])
def test_data_utils_compare_checksums(chksum1, chksum2, expected):
    assert dsf.data.utils.compare_checksums(chksum1=chksum1, chksum2=chksum2) == expected


def test_data_utils_validate_file_transfer():
    assert dsf.data.utils.validate_file_transfer(filename="test", file_transfer_status=True)


def test_data_utils_validate_file_transfer_failed():
    with pytest.raises(IOError):
        dsf.data.utils.validate_file_transfer(filename="test", file_transfer_status=False)


def test_data_hyperbot_validate_config_data_acquisition_path():
    with pytest.raises(IOError):
        dsf.data.hyperbot.validate_config(config=CONFIG_DAP_DNE)


def test_data_hyperbot_validate_config_data_acquisition_content():
    with pytest.raises(IOError):
        dsf.data.hyperbot.validate_config(config=CONFIG_DAP_CONTENT)


def test_data_hyperbot_find_scans():
    scans = dsf.data.hyperbot.find_scans(config=CONFIG_DATA)
    assert scans["vnir"]["2019-08-08__16-38-21-380"]["sample_id"] == "project_test"


def test_data_hyperbot_find_scans_no_project():
    scans = dsf.data.hyperbot.find_scans(config=CONFIG_DATA)
    assert scans["skipped"][0] == os.path.join(TEST_DATA, "acquisition", "vnir", "2019-08-08",
                                               "2019-08-08__18-38-21-380")


def test_data_hyperbot_init_scan_dirs():
    tasks = dsf.data.hyperbot.init_scan_dirs(scans=SCANS, config=CONFIG_DATA)
    assert tasks[0]["target_dir"] == os.path.join(TEST_TMPDIR, "project", "project_test_2019-08-08__16-38-21-380")


def test_data_hyperbot_move_scan_files():
    shutil.copytree(os.path.join(TEST_DATA, "acquisition"), os.path.join(TEST_TMPDIR, "acquisition"))
    os.mkdir(os.path.join(TEST_TMPDIR, "project"))
    os.mkdir(os.path.join(TEST_TMPDIR, "project", "project_test_2019-08-08__16-38-21-380"))
    dsf.data.hyperbot.move_scan_files(tasks=TASKS)
    assert os.path.exists(os.path.join(TEST_TMPDIR, "project", "project_test_2019-08-08__16-38-21-380",
                                       "vnir_26bb8db2-c4ff-4081-bca8-40b8be56ad77_metadata.json"))


def test_data_hyperbot_delete_source_scan_files():
    shutil.copytree(os.path.join(TEST_DATA, "acquisition"), os.path.join(TEST_TMPDIR, "acquisition"))
    config = deepcopy(CONFIG_DATA)
    config["data_acquisition_path"] = os.path.join(TEST_TMPDIR, "acquisition")
    dsf.data.hyperbot.delete_source_scan_files(scans=SCANS, config=config)
    assert not os.path.exists(os.path.join(TEST_TMPDIR, "acquisition", "vnir", "2019-08-08",
                                           "2019-08-08__16-38-21-380"))


def teardown_function():
    """Test teardown function."""
    shutil.rmtree(TEST_TMPDIR)
