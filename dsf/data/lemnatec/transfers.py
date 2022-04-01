import os
import zipfile
from datetime import datetime
import numpy as np
import cv2


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
                date_path = datetime.strptime(metadata[snapshot][image]["timestamp"],
                                              "%Y-%m-%d_%H:%M:%S.%f").strftime("%Y-%m-%d")
                remote_path = os.path.join("/data/pgftp", config.database, date_path, raw_img)
                _transfer_raw_image(sftp=sftp, remote_path=remote_path, local_path=local_path)
                img = _convert_raw_to_png(raw=local_path,
                                          height=metadata[snapshot][image]["height"],
                                          width=metadata[snapshot][image]["width"],
                                          dtype=config.dataformat[metadata[snapshot][image]["dataformat"]]["datatype"],
                                          imgtype=config.dataformat[metadata[snapshot][image]["dataformat"]]["imgtype"],
                                          flip=metadata[snapshot][image]["rotate_flip_type"])
                cv2.imwrite(imgpath, img)
                os.remove(local_path)


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


def _convert_raw_to_png(raw, height, width, dtype, imgtype, flip):
    """Convert the raw image to PNG format.

    Keyword arguments:
    raw = raw image file
    height = height of the image
    width = width of the image
    dtype = image data type

    """
    # Is the file a zip file?
    if zipfile.is_zipfile(raw):
        # Initialize a ZipFile object
        zf = zipfile.ZipFile(raw)
        # Open the Zip file and extract the image data
        with zf.open("data") as fp:
            # Raw image data as a string
            img_str = fp.read()
            raw = np.fromstring(img_str, dtype=dtype, count=height * width)
            img = raw.reshape((height, width))
            if imgtype == "color":
                img = cv2.cvtColor(img, cv2.COLOR_BAYER_RG2BGR)
            if flip != 0:
                img = _rotate_image(img)
            return img


def _rotate_image(img):
    """Rotate an image 180 degrees

    :param img: ndarray
    :return img: ndarray
    """
    # Flip vertically
    img = cv2.flip(img, 1)
    # Flip horizontally
    img = cv2.flip(img, 0)

    return img
