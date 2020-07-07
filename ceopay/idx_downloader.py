import boto3
from config import Aws, Edgar
import requests
from io import StringIO
import logging
import os

# todo improve logging across multiprocessors

log = logging.getLogger(__name__)


def request_single_idxfile(year: str, qtr: str) -> StringIO:
    file_url = f'{Edgar.EDGAR_ROOT}/edgar/full-index/{year}/QTR{qtr}/master.idx'
    r = requests.get(file_url)
    all_lines = StringIO(r.text).readlines()

    file_buffer = StringIO()
    headers = 'fid|Year|Quarter|' + all_lines[9].replace(' ', '')
    file_buffer.write(headers)
    for line in all_lines[11:]:
        fid = line.split('|')[4].replace('edgar/data/', '').replace('/', '-').replace('.txt\n', '')
        file_buffer.write(f'{fid}|{year}|{qtr}|{line}')

    return file_buffer


def upload_idx_csv(key: str, file_buffer: StringIO, output_to_local: bool) -> None:
    if output_to_local:
        output_path = f'../data/{key}'
        if not os.path.exists(os.path.dirname(output_path)):
            os.makedirs(os.path.dirname(output_path))
        with open(output_path, 'w') as f:
            f.write(file_buffer.getvalue())
        print(f'wrote: {key} locally to \'./data/*\'')
    else:
        session = boto3.Session(aws_access_key_id=Aws.ACCESS_KEY,
                                aws_secret_access_key=Aws.SECRET_KEY)
        s3 = session.client('s3')
        s3.put_object(Body=file_buffer.getvalue(), Bucket=Aws.OUPUT_BUCKET, Key=key)
        print(f'wrote: {key} to s3')


def main(year: str, qtr: str, output_to_local: bool) -> None:
    idx_csv = request_single_idxfile(year, qtr)
    upload_idx_csv(key=f'masteridx/year={year}/qtr={qtr}.txt',
                   file_buffer=idx_csv,
                   output_to_local=output_to_local)
