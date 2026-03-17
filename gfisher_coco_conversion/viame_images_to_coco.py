#!/usr/bin/env python
"""
Process VIAME image annotations into COCO format and upload to GCS.

This script discovers subdirectories containing images and a VIAME CSV,
downloads the directory contents, converts the annotations to COCO format, 
and publishes the resulting dataset to a destination GCS bucket.
"""

import argparse
import contextlib
import csv
import logging
import os
import subprocess
import tempfile

# Third-party imports
from google.cloud import storage  # noqa: F401
import pycocowriter  # noqa: F401
import pynoddgcs.connect  # noqa: F401
from pynoddgcs.connect import GCS
from pynoddgcs.publish import NODDCOCODataset
from viame2coco.viame2coco import viame2coco

# Constants
DESCRIPTION_TEMPLATE = "VIAME-sourced image annotations for {}"
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


def get_gcs_subdirectories(bucket, directory):
    """
    Retrieves a list of subdirectories within a given GCS path.

    Parameters
    ----------
    bucket : str
        The source GCS bucket.
    directory : str
        The directory path within the bucket.

    Returns
    -------
    list of str
        A list of full gs:// subdirectory paths.
    """
    # Ensure the directory path ends with a slash for gsutil directory listing
    base_path = directory if directory.endswith('/') else f"{directory}/"
    gcs_path = f"gs://{bucket}/{base_path}"
    logging.info("Scanning for subdirectories in %s...", gcs_path)
    
    try:
        result = subprocess.run(
            ["gsutil", "ls", gcs_path],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Filter for items that end with a slash (indicating they are directories)
        # and ensure we aren't just capturing the base path itself.
        subdirs = [
            line.strip() for line in result.stdout.split("\n") 
            if line.strip().endswith('/') and line.strip() != gcs_path
        ]
        return subdirs
    except subprocess.CalledProcessError as e:
        logging.error("Failed to list directories using gsutil: %s", e.stderr)
        raise


def load_completed_dirs(filepath):
    """
    Loads the set of successfully processed directory names from a CSV file.

    Parameters
    ----------
    filepath : str
        Path to the local tracking CSV file.

    Returns
    -------
    set
        A set of completed directory names.
    """
    completed = set()
    try:
        with open(filepath, 'r') as f:
            reader = csv.reader(f)
            completed = {row[0] for row in reader if row}
    except FileNotFoundError:
        logging.info("Tracking file '%s' not found. A new one will be created.", filepath)
        
    return completed


def append_completed_dir(filepath, dir_name):
    """
    Appends a successfully processed directory name to the tracking CSV.

    Parameters
    ----------
    filepath : str
        Path to the local tracking CSV file.
    dir_name : str
        The name of the directory that was successfully processed.
    """
    with open(filepath, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([dir_name])


def process_image_directory(
    subdir_url,
    subdir_name,
    dest_bucket,
    dest_dir
):
    """
    Downloads images and CSV, generates COCO annotations, and uploads the dataset.

    Parameters
    ----------
    subdir_url : str
        The full gs:// URL to the source subdirectory.
    subdir_name : str
        The base name of the subdirectory being processed.
    dest_bucket : str
        The name of the destination GCS bucket.
    dest_dir : str
        The destination directory path for the dataset upload.

    Returns
    -------
    bool
        True if the directory was successfully processed, False if skipped due to missing data.
    """
    logging.info("Processing image directory: %s", subdir_name)

    with tempfile.TemporaryDirectory() as temp_root:
        # Create the precisely named folder for NODDCOCODataset
        output_dir = os.path.join(temp_root, subdir_name)
        os.mkdir(output_dir)

        # Download all files from the GCS subdirectory using multiprocessing
        logging.debug("Downloading directory contents via gsutil...")
        gcs_wildcard = f"{subdir_url}*"
        try:
            subprocess.run(
                ["gsutil", "-m", "cp", gcs_wildcard, output_dir],
                capture_output=True,
                check=True
            )
        except subprocess.CalledProcessError as e:
            logging.error("Failed to download contents for %s: %s", subdir_name, e.stderr)
            raise

        with change_dir(output_dir):
            # Locate the CSV file among the downloaded images
            csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
            
            if not csv_files:
                logging.warning("No CSV annotation file found in %s. Skipping.", subdir_name)
                return False
                
            if len(csv_files) > 1:
                logging.warning("Multiple CSVs found in %s. Using the first one: %s", subdir_name, csv_files[0])
                
            csv_filename = csv_files[0]
            coco_filename = "annotations.json"

            logging.debug("Converting VIAME data to COCO format...")
            description = DESCRIPTION_TEMPLATE.format(subdir_name)
            
            # Run viame2coco without video-specific arguments
            cocodata = viame2coco(
                csv_filename,
                description,
                min_confidence=MIN_CONFIDENCE 
            )
            cocodata.to_json(coco_filename)

            logging.debug("Uploading resulting COCO dataset to GCS...")
            destination_path = f"{dest_dir}/{subdir_name}"
            
            # Upload the directory contents (images + annotations.json)
            abs_coco_filename = os.path.abspath(coco_filename)
            coco_nodd_dataset = NODDCOCODataset(
                abs_coco_filename, 
                destination_path, 
                dest_bucket
            )
            coco_nodd_dataset.upload()
            
            return True


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
  python images_export.py \\
      --source-bucket "nmfs_odp_sefsc" \\
      --source-dir "PEMD/Gulf of Mexico Reef Fish Annotated Library/For_Training_Images" \\
      --dest-bucket "nmfs_odp_hq" \\
      --dest-dir "nodd_tools/datasets/gfisher_images"
    '''
    
    parser = argparse.ArgumentParser(
        description="Convert VIAME image annotations to COCO format and publish to GCS.",
        epilog=example_text,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--source-bucket", required=True, help="Source GCS bucket name")
    parser.add_argument("--source-dir", required=True, help="Directory path within the source bucket containing image subdirectories")
    parser.add_argument("--dest-bucket", required=True, help="Destination GCS bucket name")
    parser.add_argument("--dest-dir", required=True, help="Directory path within the destination bucket")
    
    parser.add_argument(
        "--tracking-csv", 
        default="completed_imagedirs.csv", 
        help="Local CSV file path to track successfully processed directories."
    )
    
    parser.add_argument(
        "--skip", 
        nargs="*", 
        default=[],
        help="Space-separated list of directory names to skip."
    )
    
    parser.add_argument(
        "--verbose", 
        action="store_true", 
        help="Enable debug-level logging."
    )

    return parser.parse_args()


def main():
    args = parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.info("Starting VIAME Image to COCO conversion process.")

    try:
        subdirs = get_gcs_subdirectories(args.source_bucket, args.source_dir)
    except Exception:
        logging.critical("Could not list source subdirectories. Exiting.")
        return

    logging.info("Discovered %d subdirectories.", len(subdirs))

    completed = load_completed_dirs(args.tracking_csv)
    
    if args.skip:
        completed.update(args.skip)
        logging.info("Skipping %d explicitly declared directories.", len(args.skip))

    # Filter out already completed directories.
    # We extract the base directory name from the full gs:// URL (removing trailing slash first)
    to_do = {
        url: os.path.basename(url.rstrip('/')) 
        for url in subdirs 
        if os.path.basename(url.rstrip('/')) not in completed
    }
    
    logging.info("%d directories remaining in queue to process.", len(to_do))

    if not to_do:
        logging.info("All directories are complete. Exiting cleanly.")
        return

    failed = []
    skipped = []

    for subdir_url, subdir_name in to_do.items():
        try:
            success = process_image_directory(
                subdir_url=subdir_url,
                subdir_name=subdir_name,
                dest_bucket=args.dest_bucket,
                dest_dir=args.dest_dir
            )
            
            if success:
                completed.add(subdir_name)
                append_completed_dir(args.tracking_csv, subdir_name)
                logging.info("Successfully finished %s.", subdir_name)
            else:
                # Track directories that didn't have a CSV, so we don't count them as "failed"
                # but we might want to know they were skipped.
                skipped.append(subdir_name)

        except KeyboardInterrupt:
            logging.warning("Process interrupted by user (KeyboardInterrupt). Stopping...")
            failed.append(subdir_name)
            break
        except Exception as e:
            logging.error("Unhandled exception processing %s: %s", subdir_name, e)
            failed.append(subdir_name)

    # Final Summary
    successful_count = len(to_do) - len(failed) - len(skipped)
    logging.info(
        "Run Complete. %d processed successfully, %d skipped (missing data), %d failed.", 
        successful_count, len(skipped), len(failed)
    )
    
    if failed:
        logging.warning("Failed directories: %s", ", ".join(failed))


if __name__ == "__main__":
    main()
