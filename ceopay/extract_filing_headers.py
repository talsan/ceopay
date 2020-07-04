import requests
from config import Aws, Edgar
import re
import boto3
from ceopay import helpers
import pandas as pd
from io import StringIO
from datetime import datetime
import multiprocessing as mp
import logging

log = logging.getLogger(__name__)

session = boto3.Session(aws_access_key_id=Aws.ACCESS_KEY,
                        aws_secret_access_key=Aws.SECRET_KEY)

# tags to extract
TARGET_FILING_HEADER_TAGS = {
    'acceptancedatetime': {'tag': '<ACCEPTANCE-DATETIME>',
                           'from': '%Y%m%d%H%M%S',
                           'to': '%Y-%m-%d %H:%M:%S'},
    'accessionnumber': {'tag': 'ACCESSION NUMBER:',
                        'from': 'string',
                        'to': 'string'},
    'conformedsubmissiontype': {'tag': 'CONFORMED SUBMISSION TYPE:',
                                'from': 'string',
                                'to': 'string'},
    'publicdocumentcount': {'tag': 'PUBLIC DOCUMENT COUNT:',
                            'from': 'int',
                            'to': 'int'},
    'conformedperiodofreport': {'tag': 'CONFORMED PERIOD OF REPORT:',
                                'from': '%Y%m%d',
                                'to': '%Y-%m-%d'},
    'filedasofdate': {'tag': 'FILED AS OF DATE:',
                      'from': '%Y%m%d',
                      'to': '%Y-%m-%d'},
    'dateasofchange': {'tag': 'DATE AS OF CHANGE:',
                       'from': '%Y%m%d',
                       'to': '%Y-%m-%d'},
    'effectivenessdate': {'tag': 'EFFECTIVENESS DATE:',
                          'from': '%Y%m%d',
                          'to': '%Y-%m-%d'},
    'sicsdesc': {'tag': 'STANDARD INDUSTRIAL CLASSIFICATION:',
                 'from': 'string',
                 'to': 'this [not this]'},
    'sicscode': {'tag': 'STANDARD INDUSTRIAL CLASSIFICATION:',
                 'from': 'string',
                 'to': 'not this [this]'},
    'irsnum': {'tag': 'IRS NUMBER:',
               'from': 'string',
               'to': 'string'},
    'fiscalyearend': {'tag': 'FISCAL YEAR END:',
                      'from': 'int',
                      'to': 'int'},
    'stateofincorp': {'tag': 'STATE OF INCORPORATION:',
                      'from': 'string',
                      'to': 'string'}}


def get_filing_tag_content(data_def_obj, filing_header_text):
    tag_content = helpers.get_single_line_tag_contents(data_def_obj['tag'], filing_header_text)
    # print(tag_content)

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


def get_raw_filing_header_text(filing_idx):

    range_ends = ['1000', '5000', '10000', '50000', '']
    for range_end in range_ends:
        response = requests.get(f'{Edgar.EDGAR_ROOT}/{filing_idx["filename"]}',
                                headers={'Range': f'bytes=0-{range_end}'})
        filing_text = response.text
        filing_header_search = re.search('<SEC-HEADER>.*</SEC-HEADER>', filing_text, re.DOTALL)
        if filing_header_search:
            return filing_header_search.group()


def parse_filing_header(filing_idx):
    try:
        filing_header_text = get_raw_filing_header_text(filing_idx)
        tag_content = {k: get_filing_tag_content(v, filing_header_text)
                       for (k, v) in TARGET_FILING_HEADER_TAGS.items()}
        filing_info = {'fid': filing_idx['fid']}
        filing_info.update(tag_content)
    except Exception as e:
        log.warning(f'error parsing file header: {e} - {mp.current_process().pid} - {filing_idx}')
        filing_info = None
    return filing_info


def get_filing_idx(form_type, year, qtr):
    query_params = {'region': 'us-west-2', 'database': 'qcdb',
                    'bucket': 'edgaraws-athena-query-outputs', 'path': 'temp',
                    'query': 'SELECT * FROM edgaraws_masteridx where '
                             f'FormType=\'{form_type}\' '
                             f'and year={year} '
                             f'and quarter={qtr}'}
    def14a_idx = helpers.query_s3_to_df(session, query_params)
    return def14a_idx


def upload_filing_headers_csv(s3_key, filing_headers_df):
    csv_io = StringIO()
    filing_headers_df.to_csv(csv_io, sep='|', index=False, header=True)

    s3 = session.client('s3')
    s3.put_object(Body=csv_io.getvalue(), Bucket=Aws.OUPUT_BUCKET, Key=s3_key)

    print(f'pid[{mp.current_process().pid}] wrote: {s3_key}')


def main(form_type, year, qtr):
    # for a given form_type, year, and qtr ... get dataframe of N filings as rows and idx metadata as cols
    def14a_idx = get_filing_idx(form_type, year, qtr)

    # for each filing, extract contents of the header
    # store all filings for a given form_type, year, qtr as a pandas dataframe
    filing_headers = [parse_filing_header(filing_idx) for filing_idx in def14a_idx.to_dict('records')]
    filing_headers_df = pd.DataFrame(filing_headers)

    # for a given form_type, year, qtr ... upload a single csv file with N filings as rows and header info as cols
    s3_key = f'filing_metadata/formtype={helpers.s3_nameify(form_type)}/year={year}/qtr={qtr}.txt'
    upload_filing_headers_csv(s3_key, filing_headers_df)
