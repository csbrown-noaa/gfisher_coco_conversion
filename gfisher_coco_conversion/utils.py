import contextlib
import csv
import logging
import os
import subprocess

@contextlib.contextmanager
def change_dir(destination):
    """Context manager to safely switch the current working directory."""
    cwd = os.getcwd()
    os.chdir(destination)
    try:
        yield
    finally:
        os.chdir(cwd)

def get_relative_bucket_path(full_bucket_path, bucket):
    """Strips the GCS scheme and bucket name from a full bucket path."""
    prefix = f"gs://{bucket}/"
    return full_bucket_path.replace(prefix, "", 1)

def get_gcs_files(bucket, directory):
    """Retrieves a list of file paths from a GCS directory using gsutil."""
    clean_dir = directory.strip("/")
    gcs_path = f"gs://{bucket}/{clean_dir}" if clean_dir else f"gs://{bucket}"
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
        if "matched no objects" in e.stderr or e.returncode == 1:
            logging.warning("No files found at %s.", gcs_path)
            return []
        logging.error("Failed to list files using gsutil: %s", e.stderr)
        raise

def load_completed_items(filepath):
    """Loads the set of successfully processed item names from a CSV file."""
    completed = set()
    try:
        with open(filepath, 'r') as f:
            reader = csv.reader(f)
            completed = {row[0] for row in reader if row}
    except FileNotFoundError:
        logging.info("Tracking file '%s' not found. A new one will be created.", filepath)
        
    return completed

def append_completed_item(filepath, item_name):
    """Appends a successfully processed item name to the tracking CSV."""
    with open(filepath, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([item_name])
