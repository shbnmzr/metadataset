from pathlib import Path
import logging
from typing import List, Tuple

from metadataset.download.fetcher import (download_genome_file)


def save_paths(paths: List[str], out_file: Path) -> None:
    out_file.write_text('\n'.join(paths))

def download_split(split_name: str,
                   entries: List[Tuple[str, str]],
                   out_dir: Path,
                   meta_dir: Path,
                   category: str) -> None:

    logging.info(f'Downloading split {split_name.upper()}')
    out_dir.mkdir(parents=True, exist_ok=True)

    failed = []
    for ftp_path, _ in entries:
        accession = ftp_path.split('/')[-1]
        url = f'{ftp_path}/{accession}_genomic.fna.gz'
        dest = out_dir / f'{accession}_genomic.fna.gz'

        ok = download_genome_file(url, dest)

        if not ok:
            failed.append(url)


    if failed:
        (meta_dir / f'{category}_{split_name}_failed.txt').write_text('\n'.join(failed))
