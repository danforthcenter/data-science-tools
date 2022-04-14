import json
from dataclasses import dataclass


@dataclass
class Config:
    """Class for loading configuration settings."""
    username: str
    password: str
    hostname: str
    dataformat: dict
    metadata: dict
    timezone: str
    database: str = ""
    experiment: str = ""


def load_config(filename: str, database: str, experiment: str) -> Config:
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
                        dataformat=settings["dataformat"],
                        metadata=settings["metadata"],
                        timezone=settings["timezone"],
                        database=database,
                        experiment=experiment)
        return config
