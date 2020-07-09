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


def get_all_possible_yq_pairs(start_yq: str = '199301', end_yq: int = None) -> list:
    if end_yq is None:
        end_yq = helpers.get_current_yq_pair()

    yq_pairs_all = [str(yyyyqq) for yyyyqq in range(int(start_yq), int(end_yq))
                    if int(str(yyyyqq)[4:6]) <= 4]

    return yq_pairs_all


def get_unprocessed_yq_pairs(start_yq: int, end_yq: int) -> list:
    session = boto3.Session(aws_access_key_id=Aws.ACCESS_KEY,
                            aws_secret_access_key=Aws.SECRET_KEY)

    processed_yq_pairs = helpers.get_s3_yq_pairs(session, bucket=Aws.OUPUT_BUCKET, prefix='masteridx/')
    if helpers.get_current_yq_pair() in processed_yq_pairs:
        processed_yq_pairs.remove(helpers.get_current_yq_pair())

    all_possible_yq_pairs = get_all_possible_yq_pairs(start_yq, end_yq)

    unprocessed_yq_pairs = list(set(all_possible_yq_pairs) - set(processed_yq_pairs))
    unprocessed_yq_pairs.sort()

    return unprocessed_yq_pairs


def build_queue(start_yq: int, end_yq: int, overwrite: bool, local_output: str) -> list:
    if overwrite:
        yq_pair_queue = get_all_possible_yq_pairs(start_yq, end_yq)
    else:
        yq_pair_queue = get_unprocessed_yq_pairs(start_yq, end_yq)

    return [(yq_pair[0:4], yq_pair[5:6], local_output) for yq_pair in yq_pair_queue]


def main(year: str, quarter: str, local_output: str) -> None:
    idx_downloader.main(year, quarter, local_output)


if __name__ == '__main__':
    # command line arguments
    parser = argparse.ArgumentParser(description='batch downloader of quarterly master index files from SEC edgar '
                                                 'w/ option to update or overwrite existing downloads')
    parser.add_argument('--start',
                        help=f'<yyyyqq> format; Start year-qtr of index you wish to download (default=199301)')
    parser.add_argument('--end', help=f'<yyyyqq> format; End year-qtr of index you wish to download (default=present)')
    parser.add_argument('--local_output', help=f'parent directory of output; '
                                               f' if local_output == "s3," it uploads to s3 based on '
                                               f'Aws parameters set in config.py', type=str, default='s3')
    parser.add_argument('--overwrite',
                        help=f'downloads all requested master idx files (default behavior is to only process those '
                             f'files that aren\'t yet stored in S3)',
                        action='store_true')

args = parser.parse_args()

this_file = os.path.basename(__file__).replace('.py', '')
log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
logging.basicConfig(filename=f'../log/{log_id}.log', level=logging.INFO,
                    format=f'%(asctime)s - %(name)s - %(levelname)s - %(message)s')

input_queue = build_queue(args.start, args.end, args.overwrite, args.local_output)

if Local.MULTIPROCESS_ON:
    cpu_count = mp.cpu_count() if Local.MULTIPROCESS_CPUS is None else Local.MULTIPROCESS_CPUS
    pool = mp.Pool(processes=cpu_count)
    pool.starmap(main, input_queue)
else:
    for input_param in input_queue:
        main(*input_param)
