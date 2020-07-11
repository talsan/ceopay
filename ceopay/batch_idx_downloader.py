import boto3
from config import Aws, Local
import multiprocessing as mp
from ceopay.utils import helpers
import argparse
import logging
import os
from datetime import datetime
from ceopay import idx_downloader
import glob
import re

# todo improve logging across multiprocessors

log = logging.getLogger(__name__)


def get_all_possible_yq_pairs(start_yq: str = '199301', end_yq: str = None) -> list:
    if end_yq is None:
        end_yq = helpers.get_current_yq_pair()

    yq_pairs_all = [str(yyyyqq) for yyyyqq in range(int(start_yq), int(end_yq))
                    if 1 <= int(str(yyyyqq)[4:6]) <= 4]

    return yq_pairs_all


def get_unprocessed_yq_pairs(start_yq: str, end_yq: str, outputpath: str) -> list:
    session = boto3.Session(aws_access_key_id=Aws.ACCESS_KEY,
                            aws_secret_access_key=Aws.SECRET_KEY)

    prefix = 'masteridx'

    if outputpath == 's3':
        processed_yq_pairs = helpers.get_s3_yq_pairs(session, bucket=Aws.OUPUT_BUCKET, prefix=f'{prefix.rstrip("/")}/')
    else:
        processed_yq_pairs = []
        for key in glob.glob(f'{outputpath.rstrip("/")}/{prefix.rstrip("/")}/**/*.txt', recursive=True):
            y = re.findall('year=(\\d{4})', key)[0]
            q = re.findall('qtr=(\\d)', key)[0]
            processed_yq_pairs.append(y + '0' + q)

    if helpers.get_current_yq_pair() in processed_yq_pairs:
        processed_yq_pairs.remove(helpers.get_current_yq_pair())

    all_possible_yq_pairs = get_all_possible_yq_pairs(start_yq, end_yq)

    unprocessed_yq_pairs = list(set(all_possible_yq_pairs) - set(processed_yq_pairs))
    unprocessed_yq_pairs.sort()

    return unprocessed_yq_pairs


def build_queue(start_yq: str, end_yq: str, overwrite: bool, outputpath: str) -> list:
    if overwrite:
        yq_pair_queue = get_all_possible_yq_pairs(start_yq, end_yq)
    else:
        yq_pair_queue = get_unprocessed_yq_pairs(start_yq, end_yq, outputpath)

    return [(yq_pair[0:4], yq_pair[5:6], outputpath) for yq_pair in yq_pair_queue]


def main(year: str, quarter: str, outputpath: str) -> None:
    idx_downloader.main(year, quarter, outputpath)


if __name__ == '__main__':
    # command line arguments
    parser = argparse.ArgumentParser(description='batch downloader of quarterly master index files from SEC edgar '
                                                 'w/ option to update or overwrite existing downloads')
    parser.add_argument('outputpath', help=f'where to send output on local machine; if outputpath==\'s3\', output is '
                                           f'uploaded to the Aws.OUPUT_BUCKET variable defined in config.py', type = str)
    parser.add_argument('--start',
                        help=f'<yyyyqq> format; Start year-qtr of index you wish to download (default=199301)')
    parser.add_argument('--end', help=f'<yyyyqq> format; End year-qtr of index you wish to download (default=present)')
    parser.add_argument('--overwrite',
                        help=f'downloads all requested master idx files (default behavior is to only process those '
                             f'files that aren\'t yet stored in S3)',
                        action='store_true')

    args = parser.parse_args()

    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'./log/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    input_queue = build_queue(args.start, args.end, args.overwrite, args.outputpath)

    if Local.MULTIPROCESS_ON:
        cpu_count = mp.cpu_count() if Local.MULTIPROCESS_CPUS is None else Local.MULTIPROCESS_CPUS
        pool = mp.Pool(processes=cpu_count)
        pool.starmap(main, input_queue)
    else:
        for input_param in input_queue:
            main(*input_param)
