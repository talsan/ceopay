from config import Aws
import boto3
from ceopay import hdr_extractor
from ceopay.utils import helpers
from datetime import datetime
import logging
import os
import argparse
import re
import glob

# todo improve logging across multiprocessors

log = logging.getLogger(__name__)

session = boto3.Session(aws_access_key_id=Aws.ACCESS_KEY,
                        aws_secret_access_key=Aws.SECRET_KEY)


def build_yq_queue(formtype: str, outputpath: str, overwrite: bool) -> list:

    idx_prefix = 'masteridx/'
    metadata_prefix = f'filing_metadata/formtype={helpers.s3_nameify(formtype)}/'
    all_yq_pairs = []
    existing_yq_pairs = []

    if outputpath == 's3':
        all_yq_pairs = helpers.get_s3_yq_pairs(session,
                                               bucket=Aws.OUPUT_BUCKET,
                                               prefix='masteridx/')
        if not overwrite:
            existing_yq_pairs = helpers.get_s3_yq_pairs(bucket=Aws.OUPUT_BUCKET,
                                                        prefix=metadata_prefix)
    else:
        for key in glob.glob(f'{outputpath.rstrip("/")}/{idx_prefix.rstrip("/")}/**/*.txt', recursive=True):
            y = re.findall('year=(\\d{4})', key)[0]
            q = re.findall('qtr=(\\d)', key)[0]
            all_yq_pairs.append(y + '0' + q)

        if not overwrite:
            for key in glob.glob(f'{outputpath.rstrip("/")}/{metadata_prefix.rstrip("/")}/**/*.txt', recursive=True):
                y = re.findall('year=(\\d{4})', key)[0]
                q = re.findall('qtr=(\\d)', key)[0]
                existing_yq_pairs.append(y + '0' + q)

    if helpers.get_current_yq_pair() in existing_yq_pairs:
        existing_yq_pairs.remove(helpers.get_current_yq_pair())

    yq_pair_queue = list(set(all_yq_pairs) - set(existing_yq_pairs))
    return yq_pair_queue


def main(input_params_queue: list) -> None:
    for input_params in input_params_queue:
        hdr_extractor.main(**input_params)


if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser(description='batch extract metadata from filings '
                                                 'w/ option to update or overwrite existing extracts in S3')
    parser.add_argument('formtype', help='eg "DEF 14A", "10-K", "10-Q", etc', type=str)
    parser.add_argument('outputpath', help=f'where to send output on local machine; defaults to \'s3\', which '
                                           f'uploads to the config.Aws.OUPUT_BUCKET defined in config.py)', type=str)
    parser.add_argument('--overwrite',
                        help=f'overwrite filing headers that have previously been extracted and loaded into S3',
                        action='store_true')


    args = parser.parse_args()

    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'./log/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - {args.formtype} - %(message)s')

    yq_queue = build_yq_queue(args.formtype, args.outputpath, args.overwrite)
    input_params_queue = [{'formtype': args.formtype, 'yyyyqq': yyyyqq, 'outputpath': args.outputpath}
                          for yyyyqq in yq_queue]
    main(input_params_queue)
