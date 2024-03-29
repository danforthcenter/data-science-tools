#!/usr/bin/env python

import psycopg2
import psycopg2.extras
import argparse
import json
import os
import zipfile
import paramiko
import numpy as np
import cv2
from tqdm import tqdm


def options():
    parser = argparse.ArgumentParser(description='Retrieve data from a LemnaTec database.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-c", "--config", help="JSON config file.", required=True)
    parser.add_argument("-o", "--outdir", help="Output directory for results.", required=True)
    args = parser.parse_args()

    if os.path.exists(args.outdir):
        raise IOError("The directory {0} already exists!".format(args.outdir))

    return args


def main():
    # Read user options
    args = options()

    # Read the database connetion configuration file
    config = open(args.config, 'rU')
    # Load the JSON configuration data
    db = json.load(config)

    # SSH connection
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(db['hostname'], username='root', password=db['password'])
    sftp = ssh.open_sftp()

    # Make the output directory
    os.mkdir(args.outdir)

    # Create the SnapshotInfo.csv file
    csv = open(os.path.join(args.outdir, "SnapshotInfo.csv"), "w")

    # Connect to the LemnaTec database
    conn = psycopg2.connect(host=db['hostname'], user=db['username'], password=db['password'], database=db['database'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Get all snapshots
    snapshots = {}
    cur.execute("SELECT * FROM snapshot WHERE measurement_label = %s;", [db['experiment']])
    for row in cur:
        snapshots[row['id']] = row

    # Get all image metadata
    images = {}
    raw_images = {}
    cur.execute("SELECT * FROM snapshot INNER JOIN tiled_image ON snapshot.id = tiled_image.snapshot_id INNER JOIN "
                "tile ON tiled_image.id = tile.tiled_image_id")
    for row in cur:
        if row['snapshot_id'] in snapshots:
            image_name = row['camera_label'] + '_' + str(row['tiled_image_id']) + '_' + str(row['frame'])
            if row['snapshot_id'] in images:
                images[row['snapshot_id']].append(image_name)
            else:
                images[row['snapshot_id']] = [image_name]
            raw_images[image_name] = {'raw_image_oid': row['raw_image_oid'],
                                      'rotate_flip_type': row['rotate_flip_type'], 'dataformat': row['dataformat']}

    # Create SnapshotInfo.csv file
    header = ['experiment', 'id', 'plant barcode', 'car tag', 'timestamp', 'weight before', 'weight after',
              'water amount', 'completed', 'measurement label', 'tag', 'tiles']
    csv.write(','.join(map(str, header)) + '\n')

    # Stats
    total_snapshots = len(snapshots)
    total_water_jobs = 0
    total_images = 0

    for snapshot_id in tqdm(snapshots.keys()):
        # Reformat the completed field
        # if snapshots[snapshot_id]['completed'] == 't':
        #     snapshots[snapshot_id]['completed'] = 'true'
        # else:
        #     snapshots[snapshot_id]['completed'] = 'false'

        # Group all the output metadata
        snapshot = snapshots[snapshot_id]
        values = [db['experiment'], snapshot['id'], snapshot['id_tag'], snapshot['car_tag'],
                  snapshot['time_stamp'].strftime('%Y-%m-%d %H:%M:%S'), snapshot['weight_before'],
                  snapshot['weight_after'], snapshot['water_amount'], snapshot['completed'],
                  snapshot['measurement_label'], '']

        # If the snapshot also contains images, add them to the output
        if snapshot_id in images:
            values.append(';'.join(map(str, images[snapshot_id])))
            total_images += len(images[snapshot_id])
            # Create the local directory
            snapshot_dir = os.path.join(args.outdir, "snapshot" + str(snapshot_id))
            os.mkdir(snapshot_dir)

            for image in images[snapshot_id]:
                # Copy the raw image to the local directory
                remote_dir = os.path.join("/data/pgftp", db['database'],
                                          snapshot['time_stamp'].strftime("%Y-%m-%d"),
                                          "blob" + str(raw_images[image]['raw_image_oid']))
                local_file = os.path.join(snapshot_dir, "blob" + str(raw_images[image]['raw_image_oid']))
                try:
                    sftp.get(remote_dir, local_file)
                except IOError as e:
                    print("I/O error({0}): {1}. Offending file: {2}".format(e.errno, e.strerror, remote_dir))

                if os.path.exists(local_file):
                    # Is the file a zip file?
                    if zipfile.is_zipfile(local_file):
                        zf = zipfile.ZipFile(local_file)
                        zff = zf.open("data")
                        img_str = zff.read()

                        if 'VIS' in image or 'vis' in image:
                            if len(img_str) == db['vis_height'] * db['vis_width']:
                                raw = np.fromstring(img_str, dtype=np.uint8, count=db['vis_height']*db['vis_width'])
                                raw_img = raw.reshape((db['vis_height'], db['vis_width']))
                                img = cv2.cvtColor(raw_img, cv2.COLOR_BAYER_RG2BGR)
                                if raw_images[image]['rotate_flip_type'] != 0:
                                    img = rotate_image(img)
                                cv2.imwrite(os.path.join(snapshot_dir, image + ".png"), img)
                                os.remove(local_file)
                            else:
                                print("Warning: File {0} containing image {1} seems corrupted.".format(local_file,
                                                                                                       image))
                        elif 'NIR' in image or 'nir' in image:
                            raw_rescale = None
                            if raw_images[image]['dataformat'] == 4:
                                # New NIR camera data format (16-bit)
                                if len(img_str) == (db['nir_height'] * db['nir_width']) * 2:
                                    raw = np.fromstring(img_str, dtype=np.uint16,
                                                        count=db['nir_height'] * db['nir_width'])
                                    if np.max(raw) > 4096:
                                        print("Warning: max value for image {0} is greater than 4096.".format(image))
                                    raw_rescale = np.multiply(raw, 16)
                                else:
                                    print("Warning: File {0} containing image {1} seems corrupted.".format(local_file,
                                                                                                           image))
                            elif raw_images[image]['dataformat'] == 0:
                                # Old NIR camera data format (8-bit)
                                if len(img_str) == (db['nir_height'] * db['nir_width']):
                                    raw_rescale = np.fromstring(img_str, dtype=np.uint8,
                                                                count=db['nir_height'] * db['nir_width'])
                                else:
                                    print("Warning: File {0} containing image {1} seems corrupted.".format(local_file,
                                                                                                           image))
                            if raw_rescale is not None:
                                raw_img = raw_rescale.reshape((db['nir_height'], db['nir_width']))
                                if raw_images[image]['rotate_flip_type'] != 0:
                                    raw_img = rotate_image(raw_img)
                                cv2.imwrite(os.path.join(snapshot_dir, image + ".png"), raw_img)
                                os.remove(local_file)
                        else:
                            raw = np.fromstring(img_str, dtype=np.uint16, count=db['psII_height'] * db['psII_width'])
                            if np.max(raw) > 16384:
                                print("Warning: max value for image {0} is greater than 16384.".format(image))
                            raw_rescale = np.multiply(raw, 4)
                            raw_img = raw_rescale.reshape((db['psII_height'], db['psII_width']))
                            if raw_images[image]['rotate_flip_type'] != 0:
                                raw_img = rotate_image(raw_img)
                            cv2.imwrite(os.path.join(snapshot_dir, image + ".png"), raw_img)
                            os.remove(local_file)
                        zff.close()
                        zf.close()
                        # os.remove(local_file)
                    else:
                        print("Warning: the local file {0} containing image {1} is not a proper zip file.".format(
                            local_file, image))
                else:
                    print("Warning: the local file {0} containing image {1} was not copied correctly.".format(
                        local_file, image))
        else:
            values.append('')
            total_water_jobs += 1

        csv.write(','.join(map(str, values)) + '\n')

    cur.close()
    conn.close()
    sftp.close()
    ssh.close()

    print("Total snapshots = " + str(total_snapshots))
    print("Total water jobs = " + str(total_water_jobs))
    print("Total images = " + str(total_images))


def rotate_image(img):
    """Rotate an image 180 degrees

    :param img: ndarray
    :return img: ndarray
    """
    # Flip vertically
    img = cv2.flip(img, 1)
    # Flip horizontally
    img = cv2.flip(img, 0)

    return img


if __name__ == '__main__':
    main()
