from config import Aws
import boto3
from ceopay import helpers, extract_filing_headers
from datetime import datetime
import multiprocessing as mp
import logging

session = boto3.Session(aws_access_key_id=Aws.ACCESS_KEY,
                        aws_secret_access_key=Aws.SECRET_KEY)

log = logging.getLogger(__name__)

form_type = 'DEF 14A'


def get_unprocessed_yq_pairs(bucket, prefix):
    processed_yq_pairs = helpers.get_s3_yq_pairs(session, bucket, prefix)
    if helpers.get_current_yq_pair() in processed_yq_pairs:
        processed_yq_pairs.remove(helpers.get_current_yq_pair())
    all_yq_pairs = helpers.get_s3_yq_pairs(session, bucket=Aws.OUPUT_BUCKET, prefix='masteridx/')

    unprocessed_yq_pairs = list(set(all_yq_pairs) - set(processed_yq_pairs))
    unprocessed_yq_pairs.sort()

    return unprocessed_yq_pairs


def main(form_year_qtr):
    extract_filing_headers.main(**form_year_qtr)


if __name__ == "__main__":
    logging.basicConfig(filename=f'../log/get_filing_header_contents_{datetime.now().strftime("%Y%m%dT%H%M%S")}.log',
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    unprocessed_yq_pairs = get_unprocessed_yq_pairs(bucket=Aws.OUPUT_BUCKET,
                                                    prefix=f'filing_metadata/formtype={helpers.s3_nameify(form_type)}/')

    params_list = [{'form_type': form_type, 'year': str(yq_pair)[0:4], 'qtr': str(yq_pair)[5:6]}
                   for yq_pair in unprocessed_yq_pairs]

    pool = mp.Pool(processes=2)
    pool.map(main, params_list)
