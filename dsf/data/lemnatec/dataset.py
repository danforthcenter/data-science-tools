import os
import json


def init_dataset(dataset_dir, config):
    """Initialize the dataset layout.

    Keyword arguments:
    dataset_dir = Dataset directory path.
    config = Instance of the class Config.

    :param dataset_dir: str
    :param config: dsf.data.lemnatec.config.Config
    """
    # Make the dataset directory if it does not exist
    os.makedirs(dataset_dir, exist_ok=True)
    # Dataset metadata
    metadata = {
        "dataset": {
            "hostname": config.hostname,
            "database": config.database,
            "experiment": config.experiment
        },
        "environment": {},
        "images": {}
    }
    # Dataset metadata file
    metadata_file = os.path.join(dataset_dir, "metadata.json")
    # If the metadata file does not exist create an empty JSON file
    if not os.path.exists(metadata_file):
        with open(metadata_file, "w") as fp:
            json.dump(metadata, fp, indent=4)


def load_dataset(dataset_dir):
    """Load the dataset metadata.

    Keyword arguments:
    dataset_dir = Dataset directory path.

    Returns:
    meta = Dataset metadata.

    :param dataset_dir: str
    :return meta: dict
    """
    with open(os.path.join(dataset_dir, "metadata.json"), "r") as fp:
        meta = json.load(fp)
        return meta


def save_dataset(dataset_dir, metadata):
    """Save the dataset metadata.

    Keyword arguments:
    dataset_dir = Dataset directory path.
    metadata = Metadata dictionary

    :param dataset_dir: str
    :param metadata: dict
    """
    with open(os.path.join(dataset_dir, "metadata.json"), "w") as fp:
        json.dump(metadata, fp, indent=4)
