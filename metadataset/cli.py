
from metadataset.download.manager import download_category
import argparse

def main():
    parser = argparse.ArgumentParser(
        prog='metadataset',
        description='Meta-dataset download and preparation toolkit'
    )

    subparsers = parser.add_subparsers(dest='command')

    # download command
    dl = subparsers.add_parser('download', help='Download genomes from GenBank')

    dl.add_argument('--category', required=True, help='Genome category')
    dl.add_argument('--base_dir', required=True, help='Dataset directory')
    dl.add_argument('--train_cutoff', required=True, help='Dataset training cutoff in the format YYYY-MM-DD')
    dl.add_argument('--val_cutoff', required=True, help='Dataset validation cutoff in format YYYY-MM-DD')
    dl.add_argument('--test_cutoff', required=True, help='Dataset test cutoff in format YYYY-MM-DD')

    dl.add_argument('--assembly_level',
                    default='Complete Genome',
                    help='Comma-separated list of allowed assembly levels')

    dl.add_argument('--seed', type=int, default=None, help='Random seed')

    args = parser.parse_args()

    if args.command == 'download':
        download_category(args)
    else:
        parser.print_help()


if __name__=='__main__':
    main()
