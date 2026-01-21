from dataclasses import dataclass
from pathlib import Path

# Mapping of specific categories to high-level domains
CATEGORY_TO_DOMAIN = {
    "archaea": "prokaryote",
    "bacteria": "prokaryote",
    "fungi": "eukaryote",
    "protozoa": "eukaryote",
    "virus": "viral",
    "viral": "viral",
}

@dataclass
class PipelineConfig:
    """Configuration settings for the preprocessing pipeline."""
    base_dir: Path
    out_dir: Path
    category: str
    min_len: int = 1000
    max_ambig: float = 0.05
    mash_threshold: float = 0.05
    keep_unknown: bool = False
