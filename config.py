import os
from dotenv import load_dotenv

load_dotenv()

class Local:
    MULTIPROCESS_ON = True
    MULTIPROCESS_CPUS = None # None defaults to mp.cpu_count()


class Aws:
    ACCESS_KEY = os.environ.get('ACCESS_KEY')
    SECRET_KEY = os.environ.get('SECRET_KEY')

    OUPUT_BUCKET = 'edgaraws'


class Edgar:
    EDGAR_ROOT = 'https://www.sec.gov/Archives'

    # tags to extract
    FILING_HEADER_TARGET_TAGS = {
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
