import boto3
from config import Aws, Local
import multiprocessing as mp
from ceopay.utils import helpers
import argparse
import logging
import os
from datetime import datetime
from ceopay import idx_downloader

# todo improve logging across multiprocessors

log = logging.getLogger(__name__)


def get_all_possible_yq_pairs(start_year: int) -> list:
    current_yq_pair = helpers.get_current_yq_pair()

    yq_pairs_all = []
    for y in range(start_year, int(current_yq_pair[0:4]) + 1):
        for q in range(1, 5):
            new_value = str(y * 100 + q)
            if new_value <= current_yq_pair:
                yq_pairs_all.append(new_value)

    return yq_pairs_all


def get_unprocessed_yq_pairs(start_year: int) -> list:
    session = boto3.Session(aws_access_key_id=Aws.ACCESS_KEY,
                            aws_secret_access_key=Aws.SECRET_KEY)

    processed_yq_pairs = helpers.get_s3_yq_pairs(session, bucket=Aws.OUPUT_BUCKET, prefix='masteridx/')
    if helpers.get_current_yq_pair() in processed_yq_pairs:
        processed_yq_pairs.remove(helpers.get_current_yq_pair())

    all_possible_yq_pairs = get_all_possible_yq_pairs(start_year)

    unprocessed_yq_pairs = list(set(all_possible_yq_pairs) - set(processed_yq_pairs))
    unprocessed_yq_pairs.sort()

    return unprocessed_yq_pairs


def build_queue(start_year: int, overwrite: bool, output_to_local: bool) -> list:
    if overwrite:
        yq_pair_queue = get_all_possible_yq_pairs(start_year)
    else:
        yq_pair_queue = get_unprocessed_yq_pairs(start_year)

    return [(yq_pair[0:4], yq_pair[5:6], output_to_local) for yq_pair in yq_pair_queue]


def main(year: str, quarter: str, output_to_local: bool) -> None:
    idx_downloader.main(year, quarter, output_to_local)


if __name__ == '__main__':
    # command line arguments
    parser = argparse.ArgumentParser(description='Extract Raw ETF Holding Files')
    parser.add_argument('start_year', help=f'Start year of index you wish to download', type=int)
    parser.add_argument('--overwrite',
                        help=f'Overwrite year-quarter index files that have already been downloaded to S3',
                        action='store_true')
    parser.add_argument('--multiprocessing', help=f'use machines additional cores for multiprocessing; '
                                                  f'go to config to customize cpu count',
                        action='store_true')
    parser.add_argument('--output_to_local', help=f'output files to local "../data/" directory (intended for testing)',
                        action='store_true')
    args = parser.parse_args()

    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'../log/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    input_queue = build_queue(args.start_year, args.overwrite, args.output_to_local)

    if args.multiprocessing:
        cpu_count = mp.cpu_count() if Local.MULTIPROCESS_CPUS is None else Local.MULTIPROCESS_CPUS
        pool = mp.Pool(processes=cpu_count)
        pool.starmap(main, input_queue)
    else:
        for input_param in input_queue:
            main(*input_param)
