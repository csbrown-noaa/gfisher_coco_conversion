#!/usr/bin/env python
"""
Merge Multiple COCO Annotation Datasets in GCS.

This script discovers multiple COCO annotation JSON files in GCS subdirectories,
downloads them locally, merges them into a single comprehensive COCO dataset
using viame2coco/pycocowriter, and uploads the merged dataset back to GCS.
"""

import argparse
import json
import logging
import os
import subprocess
import tempfile
import uuid

# Third-party imports
import pycocowriter.coco
import pycocowriter.cocomerge
from pynoddgcs.connect import GCS


def get_relative_bucket_path(full_bucket_path, bucket):
    """
    Strips the GCS scheme and bucket name from a full bucket path.

    Parameters
    ----------
    full_bucket_path : str
        The complete GCS path (e.g., 'gs://my-bucket/path/to/file.json').
    bucket : str
        The name of the bucket to remove from the path.

    Returns
    -------
    str
        The relative path within the bucket.
    """
    prefix = f"gs://{bucket}/"
    if full_bucket_path.startswith(prefix):
        return full_bucket_path[len(prefix):]
    return full_bucket_path


def get_annotation_file_paths(bucket, directory):
    """
    Discovers subdirectories within a GCS path and constructs annotation file paths.

    Assumes that each subdirectory contains a file named 'annotations.json'.

    Parameters
    ----------
    bucket : str
        The GCS bucket name.
    directory : str
        The directory path within the bucket containing the dataset subfolders.

    Returns
    -------
    list of str
        A list of full GCS paths to the expected annotation.json files.
    """
    # Ensure directory string ends with a slash for gsutil enumeration
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
        # Filter for subdirectories and append the target filename
        files = [line.strip() for line in result.stdout.split("\n") if line.strip()]
        annotation_files = [f"{f}annotations.json" for f in files if f.endswith('/')]
        return annotation_files
        
    except subprocess.CalledProcessError as e:
        logging.error("Failed to list directories using gsutil: %s", e.stderr)
        raise


def get_annotation_paths_from_list(bucket, directory, list_file):
    """
    Reads a list of subdirectories from a file and constructs GCS annotation paths.

    Parameters
    ----------
    bucket : str
        The GCS bucket name.
    directory : str
        The directory path within the bucket containing the dataset subfolders.
    list_file : str
        Local path to a text file containing a newline-delimited list of directories.

    Returns
    -------
    list of str
        A list of full GCS paths to the expected annotation.json files.
    """
    base_path = directory if directory.endswith('/') else f"{directory}/"
    annotation_files = []
    
    logging.info("Reading subdirectories from %s...", list_file)
    with open(list_file, 'r') as f:
        for line in f:
            subdir = line.strip()
            if subdir:
                # Strip slashes to ensure clean path concatenation
                subdir = subdir.strip('/')
                full_path = f"gs://{bucket}/{base_path}{subdir}/annotations.json"
                annotation_files.append(full_path)
                
    logging.info("Constructed %d annotation paths from list.", len(annotation_files))
    return annotation_files


def merge_and_publish_coco(
    annotation_files,
    source_bucket,
    dest_path,
    coco_info,
    client
):
    """
    Downloads individual COCO files, merges them, and uploads the result.

    Parameters
    ----------
    annotation_files : list of str
        List of full GCS paths to the individual 'annotations.json' files.
    source_bucket : str
        The source GCS bucket name.
    dest_path : str
        The destination GCS path (relative to bucket) for the merged file.
    coco_info : pycocowriter.coco.COCOInfo
        Metadata object to embed into the merged COCO dataset.
    client : pynoddgcs.connect.GCS
        Active GCS client instance.
    """
    with tempfile.TemporaryDirectory() as temp_root:
        
        # We create a sub-directory for downloads to keep the workspace clean
        input_dir = os.path.join(temp_root, "inputs")
        os.mkdir(input_dir)
        
        # By placing this inside the temp_root, it guarantees exact naming
        # while taking advantage of the automatic cleanup on context exit.
        merged_filename = os.path.join(temp_root, 'annotations.json')
        
        logging.info("Downloading %d annotation files...", len(annotation_files))
        destination_files = []
        
        # 1. Download
        for annotation_file in annotation_files:
            # Assign a random UUID name locally to prevent name collisions
            dest_file = os.path.join(input_dir, f"{uuid.uuid4().hex}.json")
            destination_files.append(dest_file)
            
            relative_path = get_relative_bucket_path(annotation_file, source_bucket)
            logging.debug("Downloading %s -> %s", relative_path, os.path.basename(dest_file))
            
            # Note: This assumes all files exist. Real-world data might require a try/except here.
            client.download(source_bucket, relative_path, dest_file)

        # 2. Load
        logging.info("Loading downloaded JSONs into memory...")
        cocos = []
        for dest_file in destination_files:
            with open(dest_file, 'r') as f:
                cocos.append(json.load(f))
                
        # 3. Merge
        logging.info("Merging datasets using pycocowriter...")
        merged_coco = pycocowriter.cocomerge.coco_merge(*cocos, info=coco_info)
        
        # 4. Dump locally
        logging.info("Writing merged dataset to %s...", merged_filename)
        with open(merged_filename, 'w') as f:
            json.dump(merged_coco, f)
            
        # 5. Upload to GCS
        logging.info("Uploading merged dataset to gs://%s/%s...", source_bucket, dest_path)
        client.upload(source_bucket, merged_filename, dest_path)
        logging.info("Merge and upload complete!")


def parse_args():
    """
    Parses command line arguments.

    Returns
    -------
    argparse.Namespace
        The parsed command line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Merge multiple VIAME-COCO annotations from GCS into a single dataset."
    )

    parser.add_argument(
        "--bucket", 
        default="nmfs_odp_hq", 
        help="GCS bucket name (used for both source and destination)."
    )
    parser.add_argument(
        "--dir", 
        default="nodd_tools/datasets/gfisher", 
        help="Base directory path within the bucket containing the subdirectories."
    )
    
    parser.add_argument(
        "--input-list", 
        default=None, 
        help="Path to a text file containing a newline-delimited list of directories. Overrides bucket scanning."
    )
    parser.add_argument(
        "--output-name", 
        default="annotations.json", 
        help="Name of the output merged JSON file (e.g., train_annotations.json)."
    )
    
    # Metadata arguments
    parser.add_argument(
        "--desc", 
        default="GFISHER human annotations using VIAME, 2021-2024", 
        help="Description metadata for the merged dataset."
    )
    parser.add_argument(
        "--contributor", 
        default="CScott Brown (scott.brown@noaa.gov)", 
        help="Contributor metadata."
    )
    parser.add_argument(
        "--year", 
        default="2025", 
        help="Year metadata."
    )
    parser.add_argument(
        "--version", 
        default="0.1", 
        help="Version metadata string."
    )
    
    parser.add_argument(
        "-v", "--verbose", 
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

    logging.info("Starting COCO merge pipeline.")

    if args.input_list:
        try:
            annotation_files = get_annotation_paths_from_list(args.bucket, args.dir, args.input_list)
        except Exception as e:
            logging.critical("Failed to read input list: %s", e)
            return
    else:
        try:
            annotation_files = get_annotation_file_paths(args.bucket, args.dir)
        except Exception:
            logging.critical("Failed to resolve annotation paths from GCS. Exiting.")
            return

    if not annotation_files:
        logging.warning("No valid subdirectories or annotation files found.")
        return

    # Build the COCOInfo metadata object
    coco_info = pycocowriter.coco.COCOInfo(
        description=args.desc,
        contributor=args.contributor,
        year=args.year,
        version=args.version
    )

    client = GCS()
    
    # Ensure dest_path correctly forms `dir/output_name`
    base_dir = args.dir.rstrip('/')
    dest_path = f"{base_dir}/{args.output_name}"

    try:
        merge_and_publish_coco(
            annotation_files=annotation_files,
            source_bucket=args.bucket,
            dest_path=dest_path,
            coco_info=coco_info,
            client=client
        )
    except Exception as e:
        logging.exception("A fatal error occurred during the merge process:")


if __name__ == "__main__":
    main()
