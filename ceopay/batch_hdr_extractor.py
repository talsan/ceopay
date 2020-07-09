from config import Aws
import boto3
from ceopay import hdr_extractor
from ceopay.utils import helpers
from datetime import datetime
import logging
import os
import argparse

# todo improve logging across multiprocessors

log = logging.getLogger(__name__)

session = boto3.Session(aws_access_key_id=Aws.ACCESS_KEY,
                        aws_secret_access_key=Aws.SECRET_KEY)

def get_unprocessed_yq_pairs(bucket: str, prefix: str) -> list:
    processed_yq_pairs = helpers.get_s3_yq_pairs(session, bucket, prefix)
    if helpers.get_current_yq_pair() in processed_yq_pairs:
        processed_yq_pairs.remove(helpers.get_current_yq_pair())
    all_yq_pairs = helpers.get_s3_yq_pairs(session, bucket=Aws.OUPUT_BUCKET, prefix='masteridx/')

    unprocessed_yq_pairs = list(set(all_yq_pairs) - set(processed_yq_pairs))

    return unprocessed_yq_pairs


def build_yq_queue(formtype: str, overwrite: bool) -> list:
    if overwrite:
        yq_pair_queue = helpers.get_s3_yq_pairs(session,
                                                bucket=Aws.OUPUT_BUCKET,
                                                prefix='masteridx/')
    else:
        yq_pair_queue = get_unprocessed_yq_pairs(bucket=Aws.OUPUT_BUCKET,
                                                 prefix=f'filing_metadata/formtype={helpers.s3_nameify(formtype)}/')

    return yq_pair_queue


def main(input_params_queue: list) -> None:
    for input_params in input_params_queue:
        hdr_extractor.main(**input_params)


if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser(description='batch extract metadata from filings '
                                                 'w/ option to update or overwrite existing extracts in S3')
    parser.add_argument('formtype',help='eg "DEF 14A", "10-K", "10-Q", etc', type=str)
    parser.add_argument('--overwrite', help=f'overwrite filing headers that have previously been extracted and loaded into S3',
                        action='store_true')
    parser.add_argument('--local_output', help=f'where to send output on local machine; defaults to \'s3\', which '
                                               f'uploads to the config.Aws.OUPUT_BUCKET defined in config.py)', type=str, default='s3')

    args = parser.parse_args()

    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'../log/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - {args.formtype} - %(message)s')

    yq_queue = build_yq_queue(args.formtype, args.overwrite)
    input_params_queue = [{'formtype': args.formtype, 'yyyyqq': yyyyqq,'local_output': args.local_output}
                          for yyyyqq in yq_queue]
    main(input_params_queue)
