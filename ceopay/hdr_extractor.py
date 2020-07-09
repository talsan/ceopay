import requests
from config import Aws, Edgar, Local
import re
import boto3
from ceopay.utils import helpers
import pandas as pd
from io import StringIO
from datetime import datetime
import multiprocessing as mp
import logging
import os
import argparse

# todo improve logging across multiprocessors

log = logging.getLogger(__name__)

session = boto3.Session(aws_access_key_id=Aws.ACCESS_KEY,
                        aws_secret_access_key=Aws.SECRET_KEY)


def extract_header_tag_content(data_def_obj: dict, filing_header_text: str) -> str:
    tag_content = helpers.get_single_line_tag_contents(data_def_obj['tag'], filing_header_text)

    if tag_content in ['[]', '[', ']']:
        tag_content = ''

    if len(tag_content) == 0:
        pass
    elif '%' in data_def_obj['to']:
        tag_content = datetime.strptime(tag_content, data_def_obj['from']).strftime(data_def_obj['to'])
    elif data_def_obj['to'] == 'this [not this]':
        tag_content = re.sub('\[\d{4}\]', '', tag_content).strip()
    elif data_def_obj['to'] == 'not this [this]':
        tag_content = re.sub('\[|\]', '', re.search('\[?\\s?\d{4}\\s?\]?', tag_content).group()).strip()

    return tag_content


def request_raw_filing_header_text(file_loc: str) -> str:
    range_ends = ['1000', '5000', '10000', '50000', '']
    for range_end in range_ends:
        response = requests.get(f'{Edgar.EDGAR_ROOT}/{file_loc}',
                                headers={'Range': f'bytes=0-{range_end}'})
        filing_text = response.text
        filing_header_search = re.search('<SEC-HEADER>.*</SEC-HEADER>', filing_text, re.DOTALL)
        if filing_header_search:
            return filing_header_search.group()


def list_to_filestr(filing_headers: list) -> str:
    filing_headers_df = pd.DataFrame(filing_headers)
    metadata_csv_file = StringIO()
    filing_headers_df.to_csv(metadata_csv_file, sep='|', index=False, header=True, line_terminator='\n')
    return metadata_csv_file.getvalue()


def upload_metadata_csv(key: str, filestring: str,  local_output: str) -> None:

    if local_output == 's3':
        s3 = session.client('s3')
        s3.put_object(Body=filestring, Bucket=Aws.OUPUT_BUCKET, Key=key)
        log.info(f'pid[{mp.current_process().pid}] wrote {key} to s3')

    else:
        output_path = f'{local_output.rstrip("/")}/{key}'
        if not os.path.exists(os.path.dirname(output_path)):
            os.makedirs(os.path.dirname(output_path))
        with open(output_path, 'w') as f:
            f.write(filestring)
        log.info(f'pid[{mp.current_process().pid}] wrote locally to: ./data/{key}')


def extract_filing_header(input_param: dict) -> dict:
    fid = input_param['fid']
    filename = input_param['filename']

    try:
        filing_header_text = request_raw_filing_header_text(filename)
        tag_content = {k: extract_header_tag_content(v, filing_header_text)
                       for (k, v) in Edgar.FILING_HEADER_TARGET_TAGS.items()}
        metadata = {'fid': fid}
        metadata.update(tag_content)
        print(
            f'processid {mp.current_process().pid} -> successfully extracted meta data for fid: {fid}, with filename: {filename}')
        log.info(f'successfully extracted meta data for fid: {fid}, with filename: {filename}')
    except Exception as e:
        log.error(f'error parsing file header: {e} - {mp.current_process().pid} - {filename}')
        metadata = None
    return metadata


def get_filing_idx(formtype: str, year: str, qtr: str) -> pd.DataFrame:
    query_params = {'region': 'us-west-2', 'database': 'qcdb',
                    'bucket': 'edgaraws-athena-query-outputs', 'path': 'temp',
                    'query': 'SELECT * FROM edgaraws_masteridx where '
                             f'FormType=\'{formtype}\' '
                             f'and year={year} '
                             f'and quarter={qtr}'}
    filing_idx = helpers.query_s3_to_df(session, query_params)
    return filing_idx


def main(yyyyqq: str, formtype: str, local_output: str) -> None:

    year = yyyyqq[0:4]
    qtr = yyyyqq[5]

    # this_file = os.path.basename(__file__).replace('.py', '')
    # log_id = f'{this_file}_{year}{qtr}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    # logging.basicConfig(filename=f'../log/{log_id}.log', level=logging.INFO,
    #                     format=f'%(asctime)s - %(name)s - %(levelname)s - {formtype} - %(message)s')

    # for a given form_type, year, and qtr ... get dataframe of N filings as rows and idx metadata as cols
    filing_idx = get_filing_idx(formtype, year, qtr)

    input_params = [{'fid': filing['fid'], 'filename': filing['filename']}
                    for i, filing in filing_idx.iterrows() if i < 10]

    if Local.MULTIPROCESS_ON:
        cpu_count = mp.cpu_count() if Local.MULTIPROCESS_CPUS is None else Local.MULTIPROCESS_CPUS
        pool = mp.Pool(processes=cpu_count)
        filing_headers = pool.map(extract_filing_header, input_params)

    else:
        filing_headers = [extract_filing_header(input_param) for input_param in input_params]

    metadata_filestr = list_to_filestr(filing_headers)

    # for a given form_type, year, qtr ... upload a single csv file with N filings as rows and header info as cols
    key = f'filing_metadata/formtype={helpers.s3_nameify(formtype)}/year={year}/qtr={qtr}.txt'
    upload_metadata_csv(key, metadata_filestr, local_output)

if __name__ == '__main__':
    # command line arguments
    parser = argparse.ArgumentParser(description='extract additional metadata (contents within each filing\'s <SEC-HEADER> tags)'
                                                 ' across all filings within an individual master index file (filtered on a given \'formtype\')')
    parser.add_argument('yyyyqq', help=f'<yyyyqq> formatted year-quarter pair (eg 202001) to download from edgar',
                        type=str)
    parser.add_argument('formtype',help='eg "DEF 14A", "10-K", "10-Q", etc', type=str)
    parser.add_argument('--local_output', help=f'where to send output on local machine; defaults to \'s3\', which '
                                               f'uploads to the config.Aws.OUPUT_BUCKET defined in config.py)', type=str, default='s3')

    args = parser.parse_args()

    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'../log/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    main(args.yyyyqq, args.formtype, args.local_output)
