import logging
from pathlib import Path
from datetime import datetime

from metadataset.download.summary import download_summary, parse_summary
from metadataset.download.splits import save_paths, download_split
from metadataset.utils.logging import init_logging
from metadataset.utils.io import ensure_dir


def download_category(args):
    init_logging()

    category = args.category
    allowed_types = [x.strip() for x in args.assembly_level.split(',')]
    base_dir = Path(args.base_dir)

    train_cutoff = datetime.strptime(args.train_cutoff, '%Y-%m-%d')
    val_cutoff = datetime.strptime(args.val_cutoff, '%Y-%m-%d')
    test_cutoff = datetime.strptime(args.test_cutoff, '%Y-%m-%d')

    raw_dir = base_dir / 'raw' / category
    meta_dir = base_dir / 'metadata' / category
    ensure_dir(meta_dir)
    ensure_dir(raw_dir)

    logging.info(f'----- Downloading {category} -----')
    logging.info(f'assembly level: {allowed_types}')

    # Step 1: Download assembly_summary.txt
    summary_file = meta_dir / f'assembly_summary.txt'
    download_summary(category, summary_file)

    # Step 2: Parse into splits
    splits = parse_summary(summary_file, train_cutoff, val_cutoff, test_cutoff, allowed_types)

    # Step 3: Write ftp_files
    for split in ["train", "val", "test"]:
        paths = [p for p, _ in splits[split]]
        save_paths(paths, meta_dir / f"{split}_ftp_paths.txt")
        logging.info(f"Split {split.upper()}: {len(paths)} genomes")

    # Step 4: Download each split
    download_split('train', splits['train'], raw_dir / 'train', meta_dir, category)
    download_split('val', splits['val'], raw_dir / 'val', meta_dir, category)
    download_split('test', splits['test'], raw_dir / 'test', meta_dir, category)

    logging.info(f'----- Completed {category} -----')
