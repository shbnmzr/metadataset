from pathlib import Path
import logging
from typing import List, Tuple

from metadataset.download.fetcher import (download_genome_file)

def save_paths(paths: List[str], out_file: Path) -> None:
    out_file.write_text('\n'.join(paths))
