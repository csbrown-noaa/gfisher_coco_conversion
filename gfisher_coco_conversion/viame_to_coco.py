#!/usr/bin/env python
"""
Process VIAME annotations into COCO format and upload to GCS.

This script queries a source GCS bucket for video and CSV annotation pairs,
downloads them, converts the annotations using viame2coco, and publishes
the resulting COCO datasets to a destination GCS bucket.
"""

import argparse
import contextlib
import csv
import logging
import os
import subprocess
import tempfile

# Third-party imports retained from original script
from google.cloud import storage  # noqa: F401
import pycocowriter  # noqa: F401
import pynoddgcs.connect  # noqa: F401
from pynoddgcs.connect import GCS
from pynoddgcs.publish import NODDCOCODataset
from viame2coco.viame2coco import viame2coco

# Constants
DESCRIPTION_TEMPLATE = "VIAME-sourced annotations for {}"
MIN_CONFIDENCE = 0


@contextlib.contextmanager
def change_dir(destination):
    """
    Context manager to safely switch the current working directory.

    Parameters
    ----------
    destination : str
        The target directory path to switch to.

    Yields
    ------
    None
    """
    cwd = os.getcwd()
    os.chdir(destination)
    try:
        yield
    finally:
        os.chdir(cwd)


def get_relative_bucket_path(full_bucket_path, bucket):
    """
    Strips the GCS scheme and bucket name from a full bucket path.

    Parameters
    ----------
    full_bucket_path : str
        The complete GCS path (e.g., 'gs://my-bucket/path/to/file.csv').
    bucket : str
        The name of the bucket to remove from the path.

    Returns
    -------
    str
        The relative path within the bucket.
    """
    prefix = f"gs://{bucket}/"
    return full_bucket_path.replace(prefix, "", 1)


def get_gcs_files(bucket, directory):
    """
    Retrieves a list of file paths from a GCS directory using gsutil.

    Parameters
    ----------
    bucket : str
        The source GCS bucket.
    directory : str
        The directory path within the bucket.

    Returns
    -------
    list of str
        A list of full gs:// file paths.
    """
    gcs_path = f"gs://{bucket}/{directory}"
    logging.info("Listing files in %s...", gcs_path)
    
    try:
        result = subprocess.run(
            ["gsutil", "ls", gcs_path],
            capture_output=True,
            text=True,
            check=True
        )
        files = [line.strip() for line in result.stdout.split("\n") if line.strip()]
        return files
    except subprocess.CalledProcessError as e:
        logging.error("Failed to list files using gsutil: %s", e.stderr)
        raise


def pair_csv_and_videos(files):
    """
    Pairs corresponding CSV and Video files together based on root filename.

    Parameters
    ----------
    files : list of str
        List of all file paths retrieved from the bucket.

    Returns
    -------
    dict
        A mapping of root file names to a tuple of (csv_path, video_path).
    """
    csv_files = [f for f in files if f.endswith('.csv')]
    video_files = [f for f in files if not f.endswith('.csv')]

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


def load_completed_videos(filepath):
    """
    Loads the set of successfully processed video root names from a CSV file.

    Parameters
    ----------
    filepath : str
        Path to the local tracking CSV file.

    Returns
    -------
    set
        A set of completed video root names.
    """
    completed = set()
    try:
        with open(filepath, 'r') as f:
            reader = csv.reader(f)
            completed = {row[0] for row in reader if row}
    except FileNotFoundError:
        logging.info("Tracking file '%s' not found. A new one will be created.", filepath)
        
    return completed


def append_completed_video(filepath, video_root_name):
    """
    Appends a successfully processed video root name to the tracking CSV.

    Parameters
    ----------
    filepath : str
        Path to the local tracking CSV file.
    video_root_name : str
        The root name of the video that was successfully processed.
    """
    with open(filepath, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([video_root_name])


def process_video_pair(
    video_root_name, 
    csv_bucketpath, 
    video_bucketpath, 
    source_bucket, 
    dest_bucket, 
    dest_dir, 
    client
):
    """
    Downloads source files, generates COCO annotations, and uploads the dataset.

    Parameters
    ----------
    video_root_name : str
        The base name identifying the video/csv pair.
    csv_bucketpath : str
        The full GCS path to the source CSV file.
    video_bucketpath : str
        The full GCS path to the source video file.
    source_bucket : str
        The name of the source GCS bucket.
    dest_bucket : str
        The name of the destination GCS bucket.
    dest_dir : str
        The destination directory path for the dataset upload.
    client : pynoddgcs.connect.GCS
        The active GCS client instance.
    """
    logging.info("Processing pair: %s", video_root_name)

    with tempfile.TemporaryDirectory() as input_dir, \
         tempfile.TemporaryDirectory() as output_dir_root:

        output_dir = os.path.join(output_dir_root, video_root_name)
        os.mkdir(output_dir)

        csv_filename = os.path.join(input_dir, os.path.basename(csv_bucketpath))
        video_filename = os.path.join(input_dir, os.path.basename(video_bucketpath))
        coco_filename = "annotations.json"

        logging.debug("Downloading source files to temporary directory...")
        client.download(
            source_bucket, 
            get_relative_bucket_path(csv_bucketpath, source_bucket), 
            csv_filename
        )
        client.download(
            source_bucket, 
            get_relative_bucket_path(video_bucketpath, source_bucket), 
            video_filename
        )

        with change_dir(output_dir):
            logging.debug("Converting VIAME data to COCO format...")
            description = DESCRIPTION_TEMPLATE.format(video_root_name)
            
            cocodata = viame2coco(
                csv_filename,
                description,
                video_file=video_filename,
                video_frame_outfile_dir='.',
                min_confidence=MIN_CONFIDENCE 
            )
            cocodata.to_json(coco_filename)

            logging.debug("Uploading resulting COCO dataset to GCS...")
            destination_path = f"{dest_dir}/{video_root_name}"
            
            # Using absolute path for safety while inside the temp directory context
            abs_coco_filename = os.path.abspath(coco_filename)
            coco_nodd_dataset = NODDCOCODataset(
                abs_coco_filename, 
                destination_path, 
                dest_bucket
            )
            coco_nodd_dataset.upload()


def parse_args():
    """
    Parses command line arguments.

    Returns
    -------
    argparse.Namespace
        The parsed command line arguments.
    """
    example_text = '''
Examples:
  python notebook_export.py \\
      --source-bucket "nmfs_odp_sefsc" \\
      --source-dir "PEMD/Gulf of Mexico Reef Fish Annotated Library/For_Training" \\
      --dest-bucket "nmfs_odp_hq" \\
      --dest-dir "nodd_tools/datasets/gfisher"
    '''
    
    parser = argparse.ArgumentParser(
        description="Convert VIAME annotations to COCO format and publish to GCS.",
        epilog=example_text,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--source-bucket", required=True, help="Source GCS bucket name")
    parser.add_argument("--source-dir", required=True, help="Directory path within the source bucket")
    parser.add_argument("--dest-bucket", required=True, help="Destination GCS bucket name")
    parser.add_argument("--dest-dir", required=True, help="Directory path within the destination bucket")
    
    parser.add_argument(
        "--tracking-csv", 
        default="completed_videos.csv", 
        help="Local CSV file path to track successfully processed videos."
    )
    
    parser.add_argument(
        "--skip", 
        nargs="*", 
        default=["SC2-camera3_03-22-21_16-59-34.000NOFISH"],
        help="Space-separated list of video root names to skip (e.g. empty rows)."
    )
    
    parser.add_argument(
        "--verbose", 
        action="store_true", 
        help="Enable debug-level logging."
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.info("Starting VIAME to COCO conversion process.")

    try:
        files = get_gcs_files(args.source_bucket, args.source_dir)
    except Exception:
        logging.critical("Could not list source files. Exiting.")
        return

    pairs = pair_csv_and_videos(files)
    logging.info("Discovered %d matched CSV/Video pairs.", len(pairs))

    completed = load_completed_videos(args.tracking_csv)
    
    # Pre-add explicitly skipped files so they bypass processing
    if args.skip:
        completed.update(args.skip)
        logging.info("Skipping %d explicitly declared file(s).", len(args.skip))

    to_do = {k: v for k, v in pairs.items() if k not in completed}
    logging.info("%d pairs remaining in queue to process.", len(to_do))

    if not to_do:
        logging.info("All files are complete. Exiting cleanly.")
        return

    client = GCS()
    failed = []

    for video_root_name, (csv_bucketfile, video_bucketfile) in to_do.items():
        try:
            process_video_pair(
                video_root_name=video_root_name,
                csv_bucketpath=csv_bucketfile,
                video_bucketpath=video_bucketfile,
                source_bucket=args.source_bucket,
                dest_bucket=args.dest_bucket,
                dest_dir=args.dest_dir,
                client=client
            )
            
            # Immediately record the success to ensure progress isn't lost on early exit
            completed.add(video_root_name)
            append_completed_video(args.tracking_csv, video_root_name)
            logging.info("Successfully finished %s.", video_root_name)

        except KeyboardInterrupt:
            logging.warning("Process interrupted by user (KeyboardInterrupt). Stopping...")
            failed.append(video_root_name)
            break
        except Exception as e:
            logging.error("Unhandled exception processing %s: %s", video_root_name, e)
            failed.append(video_root_name)
            raise e

    # Final Summary
    successful_count = len(to_do) - len(failed)
    logging.info("Run Complete. %d processed successfully, %d failed.", successful_count, len(failed))
    
    if failed:
        logging.warning("Failed videos: %s", ", ".join(failed))


if __name__ == "__main__":
    main()
