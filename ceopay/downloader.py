import boto3
from config import Aws, Edgar
import requests
from io import StringIO
import multiprocessing as mp
from ceopay import helpers

session = boto3.Session(aws_access_key_id=Aws.ACCESS_KEY,
                        aws_secret_access_key=Aws.SECRET_KEY)

def main(yq_pair):
    y = str(yq_pair)[0:4]
    q = str(yq_pair)[5:6]
    file_url = f'{Edgar.EDGAR_ROOT}/edgar/full-index/{y}/QTR{q}/master.idx'
    r = requests.get(file_url)
    all_lines = StringIO(r.text).readlines()

    output = StringIO()
    headers = 'fid|Year|Quarter|' + all_lines[9].replace(' ', '')
    output.write(headers)
    for line in all_lines[11:]:
        fid = line.split('|')[4].replace('edgar/data/', '').replace('/', '-').replace('.txt\n', '')
        output.write(f'{fid}|{y}|{q}|{line}')

    s3 = session.client('s3')
    s3.put_object(Body=output.getvalue(), Bucket=Aws.OUPUT_BUCKET, Key=f'masteridx/year={y}/qtr={q}.txt')
    print(f'wrote: {file_url}')


def build_yq_pairs(start_year):
    current_yq_pair = helpers.get_current_yq_pair()

    yq_pairs_all = []
    for y in range(start_year, current_yq_pair + 1):
        for q in range(1, 5):
            new_value = y * 100 + q
            if new_value <= current_yq_pair:
                yq_pairs_all.append(new_value)
    return yq_pairs_all


if __name__ == '__main__':

    processed_yq_pairs = helpers.get_s3_yq_pairs(session, bucket=Aws.OUPUT_BUCKET, prefix='masteridx/')
    if helpers.get_current_yq_pair() in processed_yq_pairs:
        processed_yq_pairs.remove(helpers.get_current_yq_pair())

    all_yq_pairs = build_yq_pairs(start_year=2000)

    unprocessed_yq_pairs = list(set(all_yq_pairs) - set(processed_yq_pairs))
    unprocessed_yq_pairs.sort()

    pool = mp.Pool(processes=mp.cpu_count())
    pool.map(main, unprocessed_yq_pairs)
