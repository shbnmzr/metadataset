import logging
import time
import requests
from pathlib import Path
from datetime import datetime
from typing import List

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


def parse_summary(
        summary_path: Path,
        train_cutoff: datetime,
        validation_cutoff: datetime,
        test_cutoff: datetime,
        allowed_types: List[str],
) -> dict:
    """
    Parse the summary file and return FTP paths organized into splits
    """
    # Find the header line
    with summary_path.open() as f:
        header = next(
            line for line in f
            if line.startswith('#assembly_accession')
        ).strip().split('\t')
    ftp_idx = header.index('ftp_path')
    date_idx = header.index('seq_rel_date')
    type_idx = header.index('assembly_level')
    status_idx = header.index('version_status')

    splits = {
        'train': [],
        'validation': [],
        'test': [],
    }

    with summary_path.open() as f:
        for line in f:
            if line.startswith('#'):
                continue

            parts = line.strip().split('\t')
            if len(parts) <= max(ftp_idx, date_idx, type_idx, status_idx):
                continue

            assembly_type = parts[type_idx]
            version_status = parts[status_idx]
            ftp_path = parts[ftp_idx]

            if assembly_type not in allowed_types:
                continue
            if version_status != 'lastest':
                continue

            try:
                release_date = datetime.strptime(parts[date_idx], '%Y-%m-%d')
            except ValueError:
                continue

            if release_date <= train_cutoff:
                splits['train'].append((ftp_path, release_date))
            elif release_date <= validation_cutoff:
                splits['validation'].append((ftp_path, release_date))
            elif release_date <= test_cutoff:
                splits['test'].append((ftp_path, release_date))

        return splits
