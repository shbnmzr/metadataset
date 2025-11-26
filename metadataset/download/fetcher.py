import logging
import requests
import shutil
import time
from pathlib import Path

from metadataset.download.decompress import decompress_and_validate
from metadataset.download.summary import MAX_RETRIES

MAX_RETRIES = 5
RETRY_DELAY = 5

def download_genome_file(url: str, dest_path: Path) -> bool:
    """
    Download a single .fna.gz file and decompress it.
    :param url:
    :param dest_path:
    :return:
    """
    for attempt in range(MAX_RETRIES):
        try:
            tmp = dest_path.with_suffix('.tmp')

            with requests.get(url.replace('ftp://', 'https://'),
                              stream=True,
                              timeout=60) as r:
                r.raise_for_status()
                with open(tmp, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

                shutil.move(tmp, dest_path)
                return decompress_and_validate(dest_path)

        except Exception as e:
            logging.warning(f'Attempt {attempt + 1} failed for url {url}: {e}')
            time.sleep(RETRY_DELAY)

    logging.error(f'Failed to download {url}')
    return False
