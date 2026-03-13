#!/usr/bin/env python
"""
Process VIAME video annotations into kwcoco format and upload to GCS.
"""

import argparse
import logging
import os
import subprocess
import tempfile

# Third-party imports
from pynoddgcs.connect import GCS
from viame2coco.viame2kwcoco import convert_viame_to_kwcoco

# Package imports
from .utils import (
    change_dir,
    get_relative_bucket_path,
    get_gcs_files,
    load_completed_items,
    append_completed_item
)

def pair_csv_and_videos(files):
    VIDEO_EXTENSIONS = {'.mp4', '.mov', '.avi', '.mkv', '.mpg', '.mpeg'}
    csv_files = [f for f in files if f.lower().endswith('.csv')]
    
    def is_video(filename):
        return os.path.splitext(filename)[1].lower() in VIDEO_EXTENSIONS

    video_files = [f for f in files if is_video(f)]

    def get_root(filepath):
        return os.path.splitext(os.path.basename(filepath))[0]

    csv_map = {get_root(f): f for f in csv_files}
    video_map = {get_root(f): f for f in video_files}

    pairs = {}
    for root, csv_path in csv_map.items():
        if root in video_map:
            pairs[root] = (csv_path, video_map[root])
        else:
            logging.warning("No corresponding video found for CSV: %s", csv_path)
    return pairs

def process_video_pair(video_root_name, csv_bucketpath, video_bucketpath, source_bucket, dest_bucket, dest_dir, client):
    logging.info("Processing pair: %s", video_root_name)

    with tempfile.TemporaryDirectory() as input_dir, \
         tempfile.TemporaryDirectory() as output_dir_root:

        output_dir = os.path.join(output_dir_root, video_root_name)
        os.mkdir(output_dir)

        csv_filename = os.path.join(input_dir, os.path.basename(csv_bucketpath))
        video_basename = os.path.basename(video_bucketpath)
        coco_filename = f"{video_root_name}.kwcoco.json"

        client.download(source_bucket, get_relative_bucket_path(csv_bucketpath, source_bucket), csv_filename)

        with change_dir(output_dir):
            convert_viame_to_kwcoco(csv_path=csv_filename, output_json_path=coco_filename, video_name=video_basename)

            destination_path = dest_dir.strip("/")
            dest_json_uri = f"gs://{dest_bucket}/{destination_path}/{coco_filename}"
            subprocess.run(["gsutil", "cp", coco_filename, dest_json_uri], check=True)

            dest_video_uri = f"gs://{dest_bucket}/{destination_path}/{video_basename}"
            if video_bucketpath.strip("/") != dest_video_uri.strip("/"):
                logging.debug("Performing direct GCS-to-GCS copy of the video...")
                subprocess.run(["gsutil", "cp", video_bucketpath, dest_video_uri], check=True)
            else:
                logging.info("Source and destination video paths are identical. Skipping copy.")

def parse_args():
    parser = argparse.ArgumentParser(description="Convert VIAME video annotations to kwcoco format.")
    parser.add_argument("--source-bucket", required=True)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--dest-bucket", required=True)
    parser.add_argument("--dest-dir", required=True)
    parser.add_argument("--tracking-csv", default="completed_videos.csv")
    parser.add_argument("--skip", nargs="*", default=[])
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()

def main():
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    files = get_gcs_files(args.source_bucket, args.source_dir)
    pairs = pair_csv_and_videos(files)
    completed = load_completed_items(args.tracking_csv)
    if args.skip: completed.update(args.skip)

    to_do = {k: v for k, v in pairs.items() if k not in completed}
    if not to_do: return

    client = GCS()
    for root, (csv_p, vid_p) in to_do.items():
        try:
            process_video_pair(root, csv_p, vid_p, args.source_bucket, args.dest_bucket, args.dest_dir, client)
            append_completed_item(args.tracking_csv, root)
        except Exception as e:
            logging.error("Failed %s: %s", root, e)

if __name__ == "__main__":
    main()
