from Bio import SeqIO
from pathlib import Path

def is_valid_fasta(path: Path) -> bool:
    try:
        with open(path, 'r') as f:
            return any(line.startswith('>') for line in f)
    except Exception:
        return False
