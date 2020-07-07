# Source and Structure Executive Compensation from SEC Edgar Databases

### Process Overview
1. **Download Index Files**: critical metadata for each Edgar filing (company name, filing date, filing url, etc.) 
2. **Extract Filing Header**: additional metadata *inside* each filing
3. **Download [Def14A Document]**(https://www.sec.gov/fast-answers/answersproxyhtfhtm.html): annual proxy filings containing 100s of tables, one of which has executive compensation information
4. **Detect Compensation Table**: annual salary for top company executives (including base, bonus, options, etc.)
5. **Parse Table Contents** into consistently structured json files

### Project Details
##### 1. Download Index Files
###### `invoke_idx_downloader.py`
Creates a queue of `unprocessed_index_files` -- index files (stored on a quarterly basis) which have yet to be downloaded from Edgar. If `overwrite=True`, it processes every available index file after a given `startdate`. After queuing a (quarterly) list of index_files, it invokes `idx_downloader.py`. Note the current quarter is always reprocessed.

###### `idx_downloader.py`
Downloads individual quarterly index files and stores them in S3. If, `output_to_local=True`, it outputs the result to a identically structured local folder heirarchy in `./data/masteridx/..`
S3 Naming Example: `../masteridx/year=2020/qtr=4/idx.csv`

##### 2. Extract Filing Header
###### `invoke_hdr_extractor.py`
Creates a queue of `unprocessed_headers` by identifying the quarterly index files which do not yet have accompanying quarterly hdr/metadata files. If `overwrite=True`, it quees every downloaded index file. After queuing this quarterly list, it invokes `hdr_extractor.py`. Note, the current quarter is always reprocessed.

###### `hdr_extractor.py`
Loops through each filing (i.e. each row) in a given quarterly index file and extracts its header information. Header data from all filings are combined into a single quarterly csv file in S3. If, `output_to_local=True`, it outputs the result to a identically structured local folder heirarchy in `./data/filing_metadata/..`
S3 Naming Example: `../masteridx/year=2020/qtr=4/idx.csv`s

- Persists data in S3 in such a way that enables codified direct access and Athena querying facilities
### What's the use-case?
- iShares ETF holdings track well-defined indices - baskets of stocks that represent well-defined areas of the market.
- Having a history of over 300 index-tracking ETF holdings allows you to:
    1. build universes for stock-selection modeling (e.g. R3000, FTSE, etc.)
    2. proxy stock level exposures to MSCI GICS sectors and industries (via Sector ETFs)
    3. track market behavior (e.g. value vs growth)
    4. Anything else your curious quant heart desires :)
### Example Usage
```
from ishares.utils import s3_helpers, athena_helpers

# get a single file from s3
df_small = s3_helpers.get_etf_holdings('IWV', '2020-06-30')

# query a lot of data with Athena
df_big = athena_helpers.query('select * from qcdb.ishares_holdings '
                              'where etf=\'IWV\' '
                              'and asofdate between date \'2020-01-31\' and date \'2020-06-30\'')
```

### Project Details
#####  `config.py`
Contains critical AWS configuration parameters within the `Aws` class (e.g. `S3_ETF_HOLDINGS_BUCKET`, `AWS_KEY`, etc.)

#####  `ishares/build_etf_master_index.py`
This script scrapes the iShares landing page for their "universe" of ETFs. That source webpage (and resulting output) provides etf-level information (namely, inception dates and product page url's) required for downloading holding histories. Output is sent to `./data/ishares-etf-index.csv`
![./data/ishares-etf-index.csv](https://raw.githubusercontent.com/talsan/ishares/master/assets/img/ishares-etf-index.PNG)

#####  `ishares/queue_etfdownloaders.py`
This script builds a queue of events that are executed by `etfdownloader.py`. Specifically, for a given iShares ETF ticker, this script determines which holding dates need to be downloaded, based on which holdings were downloaded in prior sessions (with an `overwrite=True` parameter to re-process everything, if desired).

#####  `ishares/etfdownloader.py`
Given an ETF and a holdings date (i.e. a single "event"), this script downloads the csv, validates its structure, formats it, and uploads it to aws s3.

### S3 Storage Example

### Athena Query Output Example
