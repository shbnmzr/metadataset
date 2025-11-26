from Bio import SeqIO
from pathlib import Path

def is_valid_fasta(path: Path) -> bool:
    try:
        with open(path, 'r') as f:
            return any(line.startswith('>') for line in f)
    except Exception:
        return False


def has_valid_contig(path: Path, min_length=1000) -> bool:
    try:
        with open(path, 'r') as f:
            return any(len(rec.seq) >= min_length for rec in SeqIO.parse(f, 'fasta'))
    except Exception:
        return False
