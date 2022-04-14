import os
from copy import deepcopy
import re
from zoneinfo import ZoneInfo


def query_snapshots(db, metadata, experiment, config):
    """Query the database to retrieve all snapshot records.

    Keyword arguments:
    db = Database cursor object.
    metadata = Dataset metadata.
    experiment = Experiment/Measurement label.
    config = Instance of the class Config.

    Returns:
    meta = Updated dataset metadata

    :param db: psycopg2.extras.DictCursor
    :param metadata: dict
    :param experiment: str
    :param config: dsf.data.lemnatec.config.Config
    :return meta: dict
    """
    # Make a deep copy of the input dictionary
    meta = deepcopy(metadata)

    # Get local and UTC timezones
    local_tz, utc_tz = _get_timezones(config=config)

    # Query the database to retrieve all snapshot records for the given experiment
    db.execute("SELECT * FROM snapshot WHERE measurement_label = %s;", [experiment])
    for row in db:
        snapshot = f"snapshot{row['id']}"
        # If the snapshot has not been recorded in the dataset metadata add an empty record
        if snapshot not in metadata["environment"]:
            # Set local timezone
            timestamp = row["time_stamp"].replace(tzinfo=local_tz)
            # Convert from local time to UTC
            utc = timestamp.astimezone(utc_tz)
            meta["environment"][snapshot] = {
                "barcode": row["id_tag"],
                "cartag": row["car_tag"],
                "timestamp": utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "weight_before": row["weight_before"],
                "weight_after": row["weight_after"],
                "water_amount": row["water_amount"],
                "completed": row["completed"]
            }

    return meta


def query_images(db, metadata, experiment, config):
    """Query the database to retrieve all image records.

    Keyword arguments:
    db = Database cursor object.
    metadata = Dataset metadata.
    experiment = Experiment/Measurement label.
    config = Instance of the class Config.

    Returns:
    meta = Updated dataset metadata

    :param db: psycopg2.extras.DictCursor
    :param metadata: dict
    :param experiment: str
    :param config: dsf.data.lemnatec.config.Config
    :return meta: dict
    """
    # Make a deep copy of the input dictionary
    meta = deepcopy(metadata)

    # Get local and UTC timezones
    local_tz, utc_tz = _get_timezones(config=config)

    # Query the database to retrieve all image records
    db.execute("SELECT * FROM snapshot INNER JOIN tiled_image ON snapshot.id = tiled_image.snapshot_id INNER JOIN "
               "tile ON tiled_image.id = tile.tiled_image_id WHERE measurement_label = %s;", [experiment])
    for row in db:
        # Set local timezone
        timestamp = row["time_stamp"].replace(tzinfo=local_tz)
        # Convert from local time to UTC
        utc = timestamp.astimezone(utc_tz)
        # Construct the snapshot ID
        snapshot_id = f"snapshot{row['snapshot_id']}"
        # Snapshot date for the image path
        snapshot_date = utc.strftime("%Y-%m-%d")
        # Construct the image filename
        image_name = os.path.join(snapshot_date, snapshot_id,
                                  f"{row['camera_label']}_{row['tiled_image_id']}_{row['frame']}.png")
        if image_name not in metadata["images"]:
            meta["images"][image_name] = {
                "snapshot": snapshot_id,
                "barcode": row["id_tag"],
                "cartag": row["car_tag"],
                "timestamp": utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "camera_label": row["camera_label"],
                "tiled_image_id": row["tiled_image_id"],
                "frame": row["frame"],
                "raw_image_oid": row["raw_image_oid"],
                "rotate_flip_type": row["rotate_flip_type"],
                "dataformat": str(row["dataformat"]),
                "width": row["width"],
                "height": row["height"]
            }
            camera_meta = _parse_camera_label(config=config, camera_label=row["camera_label"])
            meta["images"][image_name].update(camera_meta)
    return meta


def _parse_camera_label(config, camera_label):
    camera_meta = {}
    for term in config.metadata:
        match = re.search(config.metadata[term], camera_label, flags=re.IGNORECASE)
        if match is not None:
            camera_meta[term] = match.groups()[0]
    return camera_meta


def _get_timezones(config):
    """"""
    local_tz = ZoneInfo(config.timezone)
    utc_tz = ZoneInfo("UTC")
    return local_tz, utc_tz
