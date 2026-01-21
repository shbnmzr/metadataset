import gzip
import logging
import re
from pathlib import Path
from Bio import SeqIO
from .config import CATEGORY_TO_DOMAIN

# Pre-compile regex for performance
REGEX_NON_ACGT = re.compile(r"[^ACGT]")
REGEX_SANITIZER = re.compile(r"[^A-Za-z0-9._-]+")

def sanitize_id(s: str) -> str:
    """Make a string safe for use as a filename."""
    return REGEX_SANITIZER.sub("_", s or "")

def get_replicon_type(description: str) -> str:
    """Classify a sequence (plasmid vs viral vs chromosomal) based on text."""
    d = (description or "").lower()
    if any(w in d for w in ["phage", "virus", "viral", "bacteriophage"]):
        return "viral"
    if "plasmid" in d:
        return "plasmid"
    if any(w in d for w in ["chromosome", "complete genome", "chromosomal"]):
        return "chromosomal"
    return "unknown"

def get_class4(category: str, description: str) -> str:
    """Determine the final 4-class label."""
    rtype = get_replicon_type(description)
    if rtype in {'plasmid', 'viral'}:
        return rtype
    return CATEGORY_TO_DOMAIN.get(category.lower(), 'unknown')

def iter_fasta(path: Path):
    """Generator that yields SeqRecords from normal or gzipped FASTA."""
    open_func = gzip.open if path.suffix == ".gz" else open
    try:
        with open_func(path, "rt", encoding="utf-8", errors="ignore") as fh:
            yield from SeqIO.parse(fh, "fasta")
    except Exception as e:
        logging.error(f"Error reading {path.name}: {e}")

def clean_sequence_string(seq_str: str) -> str:
    """Remove non-ACGT characters from a string."""
    return REGEX_NON_ACGT.sub("", seq_str.upper())
