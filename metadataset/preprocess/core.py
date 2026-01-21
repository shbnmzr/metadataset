import csv
import hashlib
import json
import logging
import shutil
import subprocess
from collections import defaultdict
from typing import Optional, Set
from Bio import SeqIO
from Bio.Seq import Seq

from .config import PipelineConfig
from .helpers import (
    sanitize_id,
    get_replicon_type,
    get_class4,
    iter_fasta,
    clean_sequence_string
)


class BioProcessor:
    def __init__(self, config: PipelineConfig):
        self.cfg = config
        self.seen_md5_global: Set[str] = set()
        self.stats = defaultdict(int)
        # Structure: host_map[assembly_id] = { ... }
        self.host_map = defaultdict(lambda: {"chromosome_accessions": [], "plasmids": {}})

        self.meta_dir = self.cfg.out_dir / "metadata"
        self.meta_dir.mkdir(parents=True, exist_ok=True)

    def is_high_quality(self, seq_str: str) -> bool:
        """Check quality metrics (ambiguity) BEFORE cleaning."""
        if not seq_str:
            return False
        # Count chars that are NOT A, C, G, T
        ambiguous_count = sum(1 for b in seq_str if b not in "ACGT")
        return (ambiguous_count / len(seq_str)) <= self.cfg.max_ambig

    def process_sequence(self, rec) -> Optional[Seq]:
        """Orchestrates cleaning, checking, and deduplication."""
        seq_upper = str(rec.seq).upper()

        if not self.is_high_quality(seq_upper):
            self.stats["skipped_low_quality"] += 1
            return None

        # Clean using helper
        cleaned_str = clean_sequence_string(seq_upper)

        if len(cleaned_str) < self.cfg.min_len:
            self.stats["skipped_short"] += 1
            return None

        # MD5 Deduplication
        seq_hash = hashlib.md5(cleaned_str.encode("utf-8")).hexdigest()
        if seq_hash in self.seen_md5_global:
            self.stats["skipped_duplicate_md5"] += 1
            return None

        self.seen_md5_global.add(seq_hash)
        return Seq(cleaned_str)

    def extract_plasmid_name(self, desc: str, rec_id: str) -> str:
        d_lower = desc.lower()
        if "plasmid" in d_lower:
            parts = d_lower.split("plasmid", 1)
            if len(parts) > 1 and parts[1].strip():
                idx = d_lower.find("plasmid") + 7
                return desc[idx:].strip()
        return rec_id

    def run_mash_dedup(self):
        """Runs external tool 'mash' for near-duplicate removal."""
        if shutil.which("mash") is None:
            logging.warning("MASH not found in PATH. Skipping near-identical deduplication.")
            return

        logging.info("Starting Mash deduplication...")
        pairs = [("train", "val"), ("train", "test"), ("val", "test")]
        cat = str(self.cfg.category)

        for ref_split, query_split in pairs:
            ref_dir = self.cfg.out_dir / ref_split / cat
            q_dir = self.cfg.out_dir / query_split / cat

            ref_files = list(ref_dir.glob("*.fna"))
            q_files = list(q_dir.glob("*.fna"))

            if not ref_files or not q_files:
                continue

            sketch_prefix = str(self.meta_dir / f"mash_{ref_split}")
            subprocess.run(
                ["mash", "sketch", "-o", sketch_prefix] + [str(p) for p in ref_files],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            ref_msh = f"{sketch_prefix}.msh"

            for q_file in q_files:
                q_sketch = str(self.meta_dir / f"temp_q")
                subprocess.run(
                    ["mash", "sketch", "-o", q_sketch, str(q_file)],
                    check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )

                res = subprocess.run(
                    ["mash", "dist", ref_msh, f"{q_sketch}.msh"],
                    capture_output=True, text=True
                )

                for line in res.stdout.strip().split("\n"):
                    if not line: continue
                    parts = line.split("\t")
                    dist = float(parts[2])

                    if dist <= self.cfg.mash_threshold:
                        logging.warning(f"Mash Dup ({dist:.4f}): Removing {q_file.name}")
                        q_file.unlink()
                        self.stats["skipped_mash_duplicate"] += 1
                        break

    def run(self):
        cat_str = str(self.cfg.category)
        manifest_path = self.meta_dir / f"{cat_str}_manifest.csv"

        with open(manifest_path, "w", newline="") as mf:
            writer = csv.DictWriter(mf, fieldnames=[
                "split", "category", "class4", "replicon_type",
                "host_assembly", "accession", "description", "path", "source_file"
            ])
            writer.writeheader()

            for split in ["train", "val", "test"]:
                input_split_dir = self.cfg.base_dir / split / cat_str

                if not input_split_dir.exists():
                    logging.warning(f"Skipping missing directory: {input_split_dir}")
                    continue

                files = sorted(list(input_split_dir.glob("*")))
                logging.info(f"Processing {split} ({len(files)} files)...")

                for fpath in files:
                    if fpath.name.startswith("."): continue

                    assembly_id = sanitize_id(fpath.stem)

                    for rec in iter_fasta(fpath):
                        desc = rec.description or rec.id
                        rtype = get_replicon_type(desc)
                        cls = get_class4(cat_str, desc)

                        if cls == "unknown" and not self.cfg.keep_unknown:
                            continue

                        processed_seq = self.process_sequence(rec)
                        if processed_seq is None:
                            continue

                        rec.seq = processed_seq

                        # Build Host Map
                        if rtype == "plasmid":
                            label = self.extract_plasmid_name(desc, rec.id)
                            self.host_map[assembly_id]["plasmids"][rec.id] = label
                        elif rtype == "chromosomal":
                            if rec.id not in self.host_map[assembly_id]["chromosome_accessions"]:
                                self.host_map[assembly_id]["chromosome_accessions"].append(rec.id)

                        # Write to file
                        out_cls_dir = self.cfg.out_dir / split / cls
                        out_cls_dir.mkdir(parents=True, exist_ok=True)

                        rec_id = sanitize_id(rec.id)
                        out_name = f"{fpath.stem}__{rec_id}.fna"
                        out_path = out_cls_dir / out_name

                        SeqIO.write([rec], out_path, "fasta")
                        self.stats["records_written"] += 1

                        writer.writerow({
                            "split": split, "category": cat_str,
                            "class4": cls, "replicon_type": rtype,
                            "host_assembly": assembly_id, "accession": rec.id,
                            "description": desc, "path": str(out_path),
                            "source_file": str(fpath)
                        })

            with open(self.meta_dir / f"{cat_str}_host_map.json", "w") as jf:
                json.dump(self.host_map, jf, indent=2)

            self.run_mash_dedup()

            logging.info("Processing Complete.")
            logging.info(f"Stats: {json.dumps(self.stats, indent=2)}")
