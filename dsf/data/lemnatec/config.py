import json
from dataclasses import dataclass


@dataclass
class Config:
    """Class for loading configuration settings."""
    username: str
    password: str
    hostname: str
    database: str
    experiment: str


def load_config(filename: str) -> Config:
    """Load configuration settings from a JSON file.

    Keyword arguments:
    filename = Filename of the configuration file.

    Returns:
    config = Instance of the class Config.

    :param filename: str
    :return config: dsf.data.lemnatec.config.Config
    """
    with open(filename, "r") as fp:
        # Load the JSON configuration data
        settings = json.load(fp)
        config = Config(username=settings["username"],
                        password=settings["password"],
                        hostname=settings["hostname"],
                        database=settings["database"],
                        experiment=settings["experiment"])
        return config
