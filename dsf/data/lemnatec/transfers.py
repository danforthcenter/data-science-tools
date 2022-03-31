import os


def transfer_images(metadata, sftp, dataset_dir, config):
    """Copy images from the database server to the dataset directory.

    Keyword arguments:
    metadata = Dataset metadata.
    sftp = paramiko SFTP connection object.
    dataset_dir = Dataset directory path.
    config = Instance of the class Config.

    :param metadata: dict
    :param sftp: paramiko.sftp_client.SFTPClient
    :param dataset_dir: str
    :param config: dsf.data.lemnatec.config.Config
    """
    for snapshot in metadata:
        # Snapshot directory path
        snapshot_dir = os.path.join(dataset_dir, snapshot)
        # Make the snapshot directory if it does not exist
        os.makedirs(snapshot_dir, exist_ok=True)
        # Loop over each image in the snapshot
        for image in metadata[snapshot]:
            # Image local path
            imgpath = os.path.join(snapshot_dir, image)
            # If the image does not exist we will transfer the raw image
            if not os.path.exists(imgpath):
                raw_img = f"blob{metadata[snapshot][image]['raw_image_oid']}"
                local_path = os.path.join(snapshot_dir, raw_img)
                remote_path = os.path.join("/data/pgftp", config.database,
                                           metadata[snapshot][image]["timestamp"].strftime("%Y-%m-%d"), raw_img)
                _transfer_raw_image(sftp=sftp, remote_path=remote_path, local_path=local_path)


def _transfer_raw_image(sftp, remote_path, local_path):
    """Transfer the raw image file.

    Keyword arguments:
    sftp = paramiko SFTP connection object.
    remote_path = remote filepath.
    local_path = local filepath.

    :param sftp: paramiko.sftp_client.SFTPClient
    :param remote_path: str
    :param local_path: str
    """
    try:
        sftp.get(remote_path, local_path)
    except IOError as e:
        print("I/O error({0}): {1}. Offending file: {2}".format(e.errno, e.strerror, remote_path))


def _convert_raw_to_png():
    """Convert the raw image to PNG format.
    """
