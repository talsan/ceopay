import os
from dotenv import load_dotenv

load_dotenv()

class Aws():
    ACCESS_KEY = os.environ.get('ACCESS_KEY')
    SECRET_KEY = os.environ.get('SECRET_KEY')

    OUPUT_BUCKET = 'edgaraws'

class Edgar():
    EDGAR_ROOT = 'https://www.sec.gov/Archives'