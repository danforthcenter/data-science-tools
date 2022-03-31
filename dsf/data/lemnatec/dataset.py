import os
import json


def init_dataset(dataset_dir):
    """Initialize the dataset layout.

    Keyword arguments:
    dataset_dir = Dataset directory path.

    :param dataset_dir: str
    """
    # Make the dataset directory if it does not exist
    os.makedirs(dataset_dir, exist_ok=True)
    # Dataset metadata
    metadata = {}
    # Dataset metadata file
    metadata_file = os.path.join(dataset_dir, "metadata.json")
    # If the metadata file does not exist create an empty JSON file
    if not os.path.exists(metadata_file):
        with open(metadata_file, "w") as fp:
            json.dump(metadata, fp)


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
        json.dump(metadata, fp)
