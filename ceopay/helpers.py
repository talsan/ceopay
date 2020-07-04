import re
import time
from io import BytesIO
import pandas as pd
from datetime import datetime
from config import Edgar
from botocore.exceptions import ClientError

def athena_query(client, params):
    response = client.start_query_execution(
        QueryString=params['query'],
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )
    return response


def get_fid_from_key(key):
    fid = re.sub('fid=|\\.|/', '', re.findall('/fid=[0-9\\-]+\\.', key)[0])
    return fid


def athena_to_s3(session, params, max_execution=180):
    client = session.client('athena', region_name=params["region"])
    execution = athena_query(client, params)
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'

    while max_execution > 0 and state in ['RUNNING', 'QUEUED']:
        max_execution = max_execution - 1
        response = client.get_query_execution(QueryExecutionId=execution_id)

        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                return False
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                filename = re.findall('.*/(.*)', s3_path)[0]
                return filename
        time.sleep(3)

    return False


def s3_to_df(session, params, s3_filename):
    s3client = session.client('s3')
    try:
        obj = s3client.get_object(Bucket=params['bucket'],
                                  Key=params['path'] + '/' + s3_filename)
    except ClientError as e:
        print(f'!!!------------------\n'
              f'ERROR: {e.response["Error"]["Code"]}\n'
              f'\tMISSING S3 FILENAME: {s3_filename}\n'
              f'\tQUERY PARAMETERS: {params}\n'
              f'------------------!!!\n')
        raise
    df = pd.read_csv(BytesIO(obj['Body'].read()), dtype=str)
    return df


def query_s3_to_df(session, params):
    s3_filename = athena_to_s3(session, params)
    if s3_filename:
        df = s3_to_df(session, params, s3_filename)
    else:
        raise Exception('not able to locate s3 query')

    cleanup(session, params)
    return df


def s3_nameify(formtype):
    return formtype.replace(' ', '').lower()


# Deletes all files in your path so use carefully!
def cleanup(session, params):
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(params['bucket'])
    for item in my_bucket.objects.filter(Prefix=params['path']):
        item.delete()


def get_single_line_tag_contents(tag, text):
    m = re.search(f'{tag}.*', text)
    if m:
        return m.group().replace(tag, '').strip()
    else:
        return ''


def build_document_url(filing_filename, document_filename):
    url1 = Edgar.EDGAR_ROOT
    url2 = re.sub('-|.txt', '', filing_filename)
    url3 = document_filename
    return url1 + '/' + url2 + '/' + url3


def get_s3_yq_pairs(session, bucket, prefix):
    s3 = session.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket,
                            'Prefix': prefix}
    page_iterator = paginator.paginate(**operation_parameters)
    processed_yq_pairs = []
    for page in page_iterator:
        if 'Contents' in page.keys():
            for content in page['Contents']:
                key = content['Key']
                if not key.endswith('/'):
                    y = int(re.findall('year=\\d{4}', key)[0].replace('year=', ''))
                    q = int(re.findall('qtr=\\d', key)[0].replace('qtr=', ''))
                    processed_yq_pairs.append(y * 100 + q)
    return processed_yq_pairs


def get_current_yq_pair():
    this_year = int(datetime.now().strftime('%Y'))
    current_yq_pair = this_year * 100 + int(datetime.now().strftime('%m')) // 4 + 1
    return current_yq_pair


def list_s3_keys(session, bucket, prefix='', suffix=''):
    s3 = session.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    keys = []
    for page in page_iterator:
        if 'Contents' in page.keys():
            for content in page['Contents']:
                key = content['Key']
                if not key.endswith('/'):
                    if key.endswith(suffix):
                        keys.append(content['Key'])
    return keys
