#!/usr/bin/env python
"""
Process VIAME image annotations into kwcoco format and upload to GCS.

This script takes one or more source GCS directories containing a single
VIAME CSV annotation file and multiple images. It converts the CSV into 
a standalone kwcoco dataset (without grouping into a video sequence) 
and copies the JSON and images to a destination GCS bucket.
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

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.bmp', '.tif', '.tiff'}


def process_image_directory(source_bucket, source_dir, dest_bucket, dest_dir, client):
    """
    Downloads the source CSV, generates kwcoco annotations, and copies images to destination.
    """
    files = get_gcs_files(source_bucket, source_dir)
    if not files:
        return False
        
    csv_files = [f for f in files if f.lower().endswith('.csv')]
    image_files = [f for f in files if os.path.splitext(f)[1].lower() in IMAGE_EXTENSIONS]

    if len(csv_files) != 1:
        logging.warning(
            "Expected exactly 1 CSV in '%s', but found %d. Skipping directory.", 
            source_dir, len(csv_files)
        )
        return False

    csv_bucketpath = csv_files[0]
    csv_basename = os.path.basename(csv_bucketpath)
    root_name = os.path.splitext(csv_basename)[0]

    logging.info("Processing directory: %s (Root: %s, Images: %d)", source_dir, root_name, len(image_files))

    destination_path = f"{dest_dir.strip('/')}/{root_name}"
    dest_folder_uri = f"gs://{dest_bucket}/{destination_path}/"

    with tempfile.TemporaryDirectory() as temp_dir:
        local_csv = os.path.join(temp_dir, csv_basename)
        coco_filename = f"{root_name}.kwcoco.json"
        
        logging.debug("Downloading source CSV...")
        client.download(
            source_bucket, 
            get_relative_bucket_path(csv_bucketpath, source_bucket), 
            local_csv
        )

        with change_dir(temp_dir):
            logging.debug("Converting VIAME data to standalone kwcoco format...")
            convert_viame_to_kwcoco(
                csv_path=csv_basename, 
                output_json_path=coco_filename, 
                video_name=None
            )

            logging.debug("Uploading resulting kwcoco dataset JSON to GCS...")
            dest_json_uri = f"{dest_folder_uri}{coco_filename}"
            try:
                subprocess.run(
                    ["gsutil", "cp", coco_filename, dest_json_uri],
                    capture_output=True,
                    text=True,
                    check=True
                )
            except subprocess.CalledProcessError as e:
                logging.error("Failed to upload kwcoco JSON via gsutil: %s", e.stderr)
                raise

    # Multi-threaded GCS-to-GCS image copy
    source_uri_base = f"gs://{source_bucket}/{source_dir.strip('/')}/"
    if source_uri_base == dest_folder_uri:
        logging.info("Source and destination paths are identical. Skipping image copy.")
    else:
        if image_files:
            logging.debug("Performing multi-threaded copy of %d images...", len(image_files))
            try:
                # Use gsutil -m cp -I to pipe all URIs directly, handling thousands of files easily
                process = subprocess.Popen(
                    ["gsutil", "-m", "cp", "-I", dest_folder_uri],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                out, err = process.communicate(input="\n".join(image_files))
                
                if process.returncode != 0:
                    logging.error("gsutil image copy failed: %s", err)
                    raise subprocess.CalledProcessError(process.returncode, "gsutil", output=out, stderr=err)
            except Exception as e:
                logging.error("Failed to copy images: %s", e)
                raise e
    
    return True


def parse_args():
    example_text = '''
Examples:
  python viame_images_to_kwcoco.py \\
      --source-bucket "nmfs_odp_sefsc" \\
      --source-dirs "PEMD/images/dataset_A" "PEMD/images/dataset_B" \\
      --dest-bucket "nmfs_odp_hq" \\
      --dest-dir "nodd_tools/datasets/gfisher_images"
    '''
    
    parser = argparse.ArgumentParser(
        description="Convert VIAME image annotations to kwcoco format and publish to GCS.",
        epilog=example_text,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--source-bucket", required=True, help="Source GCS bucket name")
    parser.add_argument(
        "--source-dirs", 
        required=True, 
        nargs="+", 
        help="One or more directory paths within the source bucket to process"
    )
    parser.add_argument("--dest-bucket", required=True, help="Destination GCS bucket name")
    parser.add_argument("--dest-dir", required=True, help="Base directory path within the destination bucket")
    
    parser.add_argument(
        "--tracking-csv", 
        default="completed_image_dirs.csv", 
        help="Local CSV file path to track successfully processed directories."
    )
    parser.add_argument(
        "--skip", 
        nargs="*", 
        default=[],
        help="Space-separated list of source directories to explicitly skip."
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug-level logging.")

    return parser.parse_args()


def main():
    args = parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.info("Starting VIAME to kwcoco image conversion process.")

    completed = load_completed_items(args.tracking_csv)
    
    if args.skip:
        completed.update(args.skip)
        logging.info("Skipping %d explicitly declared directory/ies.", len(args.skip))

    # Determine which directories still need processing
    to_do = [d for d in args.source_dirs if d not in completed]
    logging.info("%d directories queued for processing.", len(to_do))

    if not to_do:
        logging.info("All directories are complete. Exiting cleanly.")
        return

    client = GCS()
    failed = []

    for directory in to_do:
        try:
            success = process_image_directory(
                source_bucket=args.source_bucket,
                source_dir=directory,
                dest_bucket=args.dest_bucket,
                dest_dir=args.dest_dir,
                client=client
            )
            
            if success:
                completed.add(directory)
                append_completed_item(args.tracking_csv, directory)
                logging.info("Successfully finished directory: %s", directory)
            else:
                logging.warning("Directory %s was skipped due to validation failures.", directory)

        except KeyboardInterrupt:
            logging.warning("Process interrupted by user (KeyboardInterrupt). Stopping...")
            failed.append(directory)
            break
        except Exception as e:
            logging.error("Unhandled exception processing directory %s: %s", directory, e)
            failed.append(directory)

    # Final Summary
    successful_count = len(to_do) - len(failed)
    logging.info("Run Complete. %d processed successfully, %d failed.", successful_count, len(failed))
    
    if failed:
        logging.warning("Failed directories: %s", ", ".join(failed))


if __name__ == "__main__":
    main()
