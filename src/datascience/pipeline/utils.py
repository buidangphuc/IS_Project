import os
import shutil
import logging

logger = logging.getLogger(__name__)

def ensure_directory(path: str):
    """Ensures a directory exists."""
    if not os.path.exists(path):
        os.makedirs(path)
        logger.info(f"Created directory: {path}")

def save_to_lake(data: str, path: str, filename: str):
    """Saves string data to a local file (Data Lake simulation)."""
    ensure_directory(path)
    full_path = os.path.join(path, filename)
    with open(full_path, 'w') as f:
        f.write(data)
    logger.info(f"Saved data to {full_path}")

def clear_directory(path: str):
    """Removes all files in a directory."""
    if os.path.exists(path):
        shutil.rmtree(path)
        os.makedirs(path)
        logger.info(f"Cleared directory: {path}")

# HDFS helpers (if needed in future, can wrap hdfs CLI or pyspark)
def get_hdfs_path(path: str) -> str:
    """Returns a path formatted for HDFS/Local usage."""
    # For this docker setup, we might map local dirs. 
    # Return absolute path if running locally.
    return os.path.abspath(path)
