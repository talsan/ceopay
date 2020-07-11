import boto3
from config import Aws, Edgar
import requests
from io import StringIO
import logging
import os
import argparse
from datetime import datetime
import multiprocessing as mp
# todo improve logging across multiprocessors

log = logging.getLogger(__name__)


def request_single_idxfile(year: str, qtr: str) -> str:
    file_url = f'{Edgar.EDGAR_ROOT}/edgar/full-index/{year}/QTR{qtr}/master.idx'
    r = requests.get(file_url)
    all_lines = StringIO(r.text).readlines()

    file_buffer = StringIO()
    headers = 'fid|Year|Quarter|' + all_lines[9].replace(' ', '')
    file_buffer.write(headers)
    for line in all_lines[11:]:
        fid = line.split('|')[4].replace('edgar/data/', '').replace('/', '-').replace('.txt\n', '')
        file_buffer.write(f'{fid}|{year}|{qtr}|{line}')

    return file_buffer.getvalue()


def upload_idx_csv(key: str, filestr: str, outputpath: str) -> None:
    if outputpath.lower() == 's3':
        session = boto3.Session(aws_access_key_id=Aws.ACCESS_KEY,
                                aws_secret_access_key=Aws.SECRET_KEY)
        s3 = session.client('s3')
        s3.put_object(Body=filestr, Bucket=Aws.OUPUT_BUCKET, Key=key)
        print(f'pid={mp.current_process().pid} wrote: {key} to s3 bucket {Aws.OUPUT_BUCKET}')

    else:
        output_path = f'{outputpath.rstrip("/")}/{key}'
        if not os.path.exists(os.path.dirname(output_path)):
            os.makedirs(os.path.dirname(output_path))
        with open(output_path, 'w') as f:
            f.write(filestr)
        print(f'pid={mp.current_process().pid} wrote: {key} locally to {outputpath}')


def main(year: str, qtr: str, outputpath: str) -> None:
    idx_filestr = request_single_idxfile(year, qtr)
    upload_idx_csv(key=f'masteridx/year={year}/qtr={qtr}.txt',
                   filestr=idx_filestr,
                   outputpath=outputpath)


if __name__ == '__main__':
    # command line arguments
    parser = argparse.ArgumentParser(description='downloads individual quarterly master index files from SEC edgar')
    parser.add_argument('yyyyqq', help=f'<yyyyqq> formatted year-quarter pair (eg 202001) to download from edgar',
                        type=str)
    parser.add_argument('outputpath', help=f'where to send output on local machine; if outputpath==\'s3\', output is '
                                             f'uploaded to the Aws.OUPUT_BUCKET variable defined in config.py', type=str)

    args = parser.parse_args()

    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'./log/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    main(args.yyyyqq[0:4], args.yyyyqq[5], args.outputpath)
