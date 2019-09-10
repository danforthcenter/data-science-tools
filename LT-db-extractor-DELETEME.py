#!/usr/bin/env python

import psycopg2
import psycopg2.extras
import argparse
import json
import os
import sys
import zipfile
import paramiko
import numpy as np
import cv2
import datetime
import fnmatch
from tqdm import tqdm

def options():
    parser = argparse.ArgumentParser(description='Retrieve data from a LemnaTec database.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-c", "--config", help="JSON config file.", required=True)
    parser.add_argument("-e", "--exper", help="Experiment number (measurement label)", required=True)
    parser.add_argument("-o", "--outdir", help="Output directory for results.", required=True)
    args = parser.parse_args()

    # Try to make output directory, throw an error and quit if it already exists.

    try: os.mkdir(args.outdir)
    except Exception:
        print("The directory {0} already exists!".format(args.outdir))
        quit()
    return args

def main():
    # Read user options
    args = options()

    # Read the database connetion configuration file
    config = open(args.config, 'r')
    # Load the JSON configuration data
    db = json.load(config)
    #Load the experiment number
    exp = "'"+ args.exper + "'"

    # SSH connection
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(db['hostname'], username='root', password=db['password'])
    sftp = ssh.open_sftp()

    # Generate time stamp (as a sequence) for csv file
    now = datetime.datetime.now()
    time = now.strftime("%Y%m%d%H%M%S")

    # Create the SnapshotInfo.csv file
    csv = open(os.path.join(args.outdir, args.exper+"_SnapshotInfo_"+time+".csv"), "w")

    # Connect to the LemnaTec database
    conn = psycopg2.connect(host=db['hostname'], user=db['username'], password=db['password'], database=db['database'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Get all snapshots
    snapshots = {}
    sql_command = str("SELECT * FROM snapshot WHERE measurement_label = " + str(exp))
    cur.execute(sql_command)
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
        values = [str(exp), snapshot['id'], snapshot['id_tag'], snapshot['car_tag'],
                  snapshot['time_stamp'].strftime('%Y-%m-%d %H:%M:%S'), snapshot['weight_before'],
                  snapshot['weight_after'], snapshot['water_amount'], snapshot['propagated'],
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
                remote_dir = os.path.join(db['path'], db['database'],
                                          snapshot['time_stamp'].strftime("%Y-%m-%d"),
                                          "blob" + str(raw_images[image]['raw_image_oid']))
                local_file = os.path.join(snapshot_dir, "blob" + str(raw_images[image]['raw_image_oid']))

                try:
                    # Fix file paths for Windows systems, leave for all others
                    if sys.platform.startswith('win'):
                        local_file_win = local_file.replace('\\', '/')
                        remote_dir_win = remote_dir.replace('\\', '/')
                        sftp.get(remote_dir_win, local_file_win)

                    else:
                        sftp.get(remote_dir, local_file)

                except IOError as e:
                    print("I/O error({0}): {1}. Offending file: {2}".format(e.errno, e.strerror, remote_dir))

                if os.path.exists(local_file):
                    # Is the file a zip file?
                    if zipfile.is_zipfile(local_file):
                        zf = zipfile.ZipFile(local_file)
                        zff = zf.open("data")
                        img_str = zff.read()

                        if 'VIS' in image or 'vis' in image or 'TV' in image:
                            if len(img_str) == db['vis_height'] * db['vis_width']:
                                raw = np.frombuffer(img_str, dtype=np.uint8, count=db['vis_height']*db['vis_width'])
                                raw_img = raw.reshape((db['vis_height'], db['vis_width']))
                                colour = db['colour']
                                colour_int = int(colour)
                                print("Colour: " + colour)
                                exit()

if __name__ == '__main__':
    main()