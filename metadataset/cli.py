from metadataset.download.manager import download_category
from .preprocess import BioProcessor, PipelineConfig, CATEGORY_TO_DOMAIN

import sys
import argparse
import logging
from pathlib import Path


def run_process(args):
    """Handler for the process command."""
    cat = args.category.lower()
    if cat == "virus": cat = "viral"

    if cat not in CATEGORY_TO_DOMAIN:
        logging.error(f"Invalid category '{cat}'. Must be: {list(CATEGORY_TO_DOMAIN.keys())}")
        sys.exit(1)

    config = PipelineConfig(
        base_dir=args.base_dir,
        out_dir=args.out_dir,
        category=cat,
        min_len=args.min_len,
        max_ambig=args.max_ambig,
        mash_threshold=args.mash_threshold,
        keep_unknown=args.keep_unknown
    )

    logging.info(f"Starting processing for {cat}...")
    processor = BioProcessor(config)
    processor.run()


def main():
    parser = argparse.ArgumentParser(
        prog='metadataset',
        description='Meta-dataset download and preparation toolkit'
    )

    # 1. Create the subparsers bucket ONCE
    subparsers = parser.add_subparsers(dest='command', required=True)

    # -------------------------------------------------------
    # 2. Register 'download' command
    # -------------------------------------------------------
    dl = subparsers.add_parser('download', help='Download genomes from GenBank')
    dl.add_argument('--category', required=True, help='Genome category')
    dl.add_argument('--base_dir', required=True, help='Dataset directory')
    dl.add_argument('--train_cutoff', required=True, help='YYYY-MM-DD')
    dl.add_argument('--val_cutoff', required=True, help='YYYY-MM-DD')
    dl.add_argument('--test_cutoff', required=True, help='YYYY-MM-DD')
    dl.add_argument('--assembly_level', default='Complete Genome', help='Comma-separated list')
    dl.add_argument('--seed', type=int, default=None, help='Random seed')

    # Map this command to the download function
    dl.set_defaults(func=download_category)

    # -------------------------------------------------------
    # 3. Register 'process' command
    # -------------------------------------------------------
    proc_parser = subparsers.add_parser("process", help="Clean, Deduplicate, and Relabel data")
    proc_parser.add_argument("--base_dir", required=True, type=Path, help="Input directory")
    proc_parser.add_argument("--out_dir", required=True, type=Path, help="Output directory")
    proc_parser.add_argument("--category", required=True, help="Taxonomic category")
    proc_parser.add_argument("--min_len", type=int, default=1000)
    proc_parser.add_argument("--max_ambig", type=float, default=0.05)
    proc_parser.add_argument("--mash_threshold", type=float, default=0.05)
    proc_parser.add_argument("--keep_unknown", action="store_true")

    # Map this command to the process function
    proc_parser.set_defaults(func=run_process)

    # -------------------------------------------------------
    # 4. Parse args and Execute
    # -------------------------------------------------------
    args = parser.parse_args()

    # Configure logging based on args if needed, or default
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
