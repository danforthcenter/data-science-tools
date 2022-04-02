import os
import zipfile
from datetime import datetime
import numpy as np
import cv2
from tqdm import tqdm
import math
import sys


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
    for snapshot in tqdm(metadata.keys()):
        # We only need to transfer images if the snapshot contains images
        if len(metadata[snapshot]["images"]) > 0:
            # Snapshot directory path
            snapshot_date = datetime.strptime(metadata[snapshot]["timestamp"], "%Y-%m-%d_%H:%M:%S.%f").strftime("%Y-%m-%d")
            snapshot_dir = os.path.join(dataset_dir, snapshot_date, snapshot)
            # Make the snapshot directory if it does not exist
            os.makedirs(snapshot_dir, exist_ok=True)
            # Loop over each image in the snapshot
            for image in metadata[snapshot]["images"]:
                # Image local path
                imgpath = os.path.join(snapshot_dir, image)
                # If the image does not exist we will transfer the raw image
                if not os.path.exists(imgpath):
                    raw_img = f"blob{metadata[snapshot]['images'][image]['raw_image_oid']}"
                    local_path = os.path.join(snapshot_dir, raw_img)
                    remote_path = os.path.join("/data/pgftp", config.database, snapshot_date, raw_img)
                    _transfer_raw_image(sftp=sftp, remote_path=remote_path, local_path=local_path)
                    img_metadata = metadata[snapshot]["images"][image]
                    img = _convert_raw_to_png(raw=local_path, filename=image,
                                              height=img_metadata["height"], width=img_metadata["width"],
                                              dtype=config.dataformat[img_metadata["dataformat"]]["datatype"],
                                              imgtype=config.dataformat[img_metadata["dataformat"]]["imgtype"],
                                              precision=config.dataformat[img_metadata["dataformat"]]["bit-precision"],
                                              flip=img_metadata["rotate_flip_type"])
                    if img is not False:
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
        print(f"I/O error({e.errno}): {e.strerror}. Offending file: {remote_path}", file=sys.stderr)


def _convert_raw_to_png(raw, filename, height, width, dtype, imgtype, precision, flip):
    """Convert the raw image to PNG format.

    Keyword arguments:
    raw = raw image file
    filename = image filename
    height = height of the image
    width = width of the image
    precision = precision (bits) of the raw image values
    dtype = image data type
    flip = flag indicating whether to rotate and flip the image or not

    :param raw: str
    :param filename: str
    :param height: int
    :param width: int
    :param precision: int
    :param dtype: str
    :param flip: int
    """
    # Is the file a zip file?
    if zipfile.is_zipfile(raw):
        # Initialize a ZipFile object
        zf = zipfile.ZipFile(raw)
        # Open the Zip file and extract the image data
        with zf.open("data") as fp:
            # Raw image data as a string
            img_str = fp.read()
            # Do a QC check before attempting to convert from raw
            if _raw_qc(img_str=img_str, height=height, width=width, precision=precision, local_path=raw, filename=filename):
                # Convert the image string into a linear array of the correct data type
                raw = np.fromstring(img_str, dtype=dtype, count=height * width)
                # Reshape the linear array into a 2-d array
                img = raw.reshape((height, width))
                # Rescale the image (if needed) to cover the gap between the datatype and data precision
                img = _rescale_raw(raw_img=img, dtype=dtype, precision=precision, filename=filename)
                if imgtype == "color":
                    # Convert the Bayer filter raw image into color (BGR)
                    img = cv2.cvtColor(img, cv2.COLOR_BAYER_RG2BGR)
                if flip != 0:
                    # Rotate and flip the image if needed
                    img = _rotate_image(img)
                return img
    return False


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


def _raw_qc(img_str, height, width, local_path, filename):
    # Divide the raw image string by the total pixels
    ratio = len(img_str) / (height * width)
    # The ratio should be 1 (8-bit) or 2 (16-bit)
    if not math.isclose(ratio, 1) or not math.isclose(ratio, 2):
        print(f"Warning: the raw file {local_path} containing image {filename} is corrupted.", file=sys.stderr)
        return False
    return True


def _rescale_raw(raw_img, dtype, precision, filename):
    # The max value of the image should not exceed the max value of the data precision
    if np.max(raw_img) > (2 ** precision) - 1:
        print(f"Warning: the max value for {filename} exceeds the image's data precision of ({precision}-bit).",
              file=sys.stderr)
    # Convert the data type into the number of bits per pixel value
    store_bits = getattr(np, dtype)(0).nbytes * 8
    # Calculate the multiplication factor to scale the image by the
    # difference between the data precision and the datatype precision
    factor = 2 ** (store_bits - precision)
    raw_rescale = np.multiply(raw_img, factor)
    return raw_rescale
