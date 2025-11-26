import logging
import time
import requests
from pathlib import Path
from datetime import datetime
from typing import Tuple, List

MAX_RETRIES = 5
RETRY_DELAY = 5

def download_summary(category: str, dest: Path) -> None:
    """
    Download the assembly_summary.txt file for a given GenBank category.
    :param category: GenBank category
    :param dest: destination directory
    :return: None
    """

    url = f"https://ftp.ncbi.nlm.nih.gov/genomes/genbank/{category}/assembly_summary.txt"
    for attempt in range(MAX_RETRIES):
        try:
            logging.info(f"Downloading {url}")
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            dest.write_text(response.text)
            return
        except requests.exceptions.RequestException as e:
            logging.error(f'Attempt {attempt + 1} failed: {e}')
            time.sleep(RETRY_DELAY)

        raise RuntimeError(f'Failed to download {url} after {MAX_RETRIES} retries.')
