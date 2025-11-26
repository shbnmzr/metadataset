import logging
import gzip
import shutil
from pathlib import Path

from metadataset.download.validate import (is_valid_fasta, has_valid_contig)

def decompress_and_validate(gz_path: Path) -> bool:
    try:
        out_path = gz_path.with_suffix('')

        with gzip.open(gz_path, 'rb') as f_in ,open(out_path, 'wb') as f_out:
            shutil.copyfileobj(f_in , f_out)

        gz_path.unlink()

        if(out_path.stat().st_size < 500 or
        not has_valid_contig(out_path) or
        not is_valid_fasta(out_path)):
            logging.warning(f'Invalid FASTA file {out_path.name}')
            out_path.unlink()
            return False

        return True
    except Exception as e:
        logging.warning(f'Failed to decompress {gz_path}: {e}')
        return False
