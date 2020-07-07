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


def build_queue(formtype: str, overwrite: bool, output_to_local: bool) -> list:
    if overwrite:
        yq_pair_queue = helpers.get_s3_yq_pairs(session,
                                                bucket=Aws.OUPUT_BUCKET,
                                                prefix='masteridx/')
    else:
        yq_pair_queue = get_unprocessed_yq_pairs(bucket=Aws.OUPUT_BUCKET,
                                                 prefix=f'filing_metadata/formtype={helpers.s3_nameify(formtype)}/')

    input_params_queue = [{'formtype': args.formtype, 'yq_pair': yq_pair,
                           'multiprocess': args.multiprocess, 'output_to_local': output_to_local}
                          for yq_pair in yq_pair_queue]
    return input_params_queue


def main(input_params_queue: list) -> None:
    for input_params in input_params_queue:
        hdr_extractor.main(**input_params)


if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser(description='Extract Raw ETF Holding Files')
    parser.add_argument('formtype', help=f'which formtype (10K, 10Q, DEF 14A, etc.) metadata do you wish to extract')
    parser.add_argument('--overwrite', help=f'Overwrite holdings that have already been downloaded to S3',
                        action='store_true')
    parser.add_argument('--multiprocess', help=f'use machines additional cores for multiprocessing',
                        action='store_true')
    parser.add_argument('--output_to_local', help=f'output files to local "../data/" directory (meant for testing)',
                        action='store_true')
    args = parser.parse_args()

    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'../log/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - {args.formtype} - %(message)s')

    input_params_queue = build_queue(args.formtype, args.overwrite, args.output_to_local)

    main(input_params_queue)
