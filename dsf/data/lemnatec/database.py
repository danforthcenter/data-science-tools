from copy import deepcopy


def query_snapshots(db, config, metadata):
    """Query the database to retrieve all snapshot records.

    Keyword arguments:
    db = Database cursor object.
    config = Instance of the class Config.
    metadata = Dataset metadata.

    Returns:
    meta = Updated dataset metadata

    :param db: psycopg2.extras.DictCursor
    :param config: dsf.data.lemnatec.config.Config
    :param metadata: dict
    :return meta: dict
    """
    # Make a deep copy of the input dictionary
    meta = deepcopy(metadata)

    # Query the database to retrieve all snapshot records for the given experiment
    db.execute("SELECT * FROM snapshot WHERE measurement_label = %s;", [config.experiment])
    for row in db:
        snapshot = f"snapshot{row['id']}"
        # If the snapshot has not been recorded in the dataset metadata add an empty record
        if snapshot not in metadata:
            meta[snapshot] = {}

    return meta


def query_images(db, config, metadata):
    """Query the database to retrieve all image records.

    Keyword arguments:
    db = Database cursor object.
    config = Instance of the class Config.
    metadata = Dataset metadata.

    Returns:
    meta = Updated dataset metadata

    :param db: psycopg2.extras.DictCursor
    :param config: dsf.data.lemnatec.config.Config
    :param metadata: dict
    :return meta: dict
    """
    # Make a deep copy of the input dictionary
    meta = deepcopy(metadata)
    # Query the database to retrieve all image records
    db.execute("SELECT * FROM snapshot INNER JOIN tiled_image ON snapshot.id = tiled_image.snapshot_id INNER JOIN "
               "tile ON tiled_image.id = tile.tiled_image_id WHERE measurement_label = %s;", [config.experiment])
    for row in db:
        # If the image snapshot ID is in metadata then it belongs in this dataset
        snapshot_id = f"snapshot{row['snapshot_id']}"
        if snapshot_id in metadata:
            # Construct the image filename
            image_name = f"{row['camera_label']}_{row['tiled_image_id']}_{row['frame']}.png"
            if image_name not in metadata[snapshot_id]:
                meta[snapshot_id][image_name] = {
                    "barcode": row["id_tag"],
                    "cartag": row["car_tag"],
                    "timestamp": row["time_stamp"].strftime("%Y-%m-%d_%H:%M:%S.%f"),
                    "weight_before": row["weight_before"],
                    "weight_after": row["weight_after"],
                    "water_amount": row["water_amount"],
                    "completed": row["completed"],
                    "camera_label": row["camera_label"],
                    "tiled_image_id": row["tiled_image_id"],
                    "frame": row["frame"],
                    "raw_image_oid": row["raw_image_oid"],
                    "rotate_flip_type": row["rotate_flip_type"],
                    "dataformat": row["dataformat"]
                }
    return meta
