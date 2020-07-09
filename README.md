# Source & Structure Executive Compensation from SEC Edgar
## Process Overview
#### Generalized for any Edgar Document (10-K, 10-Q, DEF 14A, etc.)
1. **Download Master Index Files**: process critical lookup index for each Edgar filing (company name, filing date, filing url, etc.) 
2. **Extract Filing Headers**: additional filing metadata (contents within filing's `<SEC_HEADER>` tag)
3. **Download Filings**: in this case, the annual proxy filings (`formtype="DEF 14A"`) 

#### Specific to Executive Compensation (Form DEF 14A)
1. **Detect Compensation Table**: Random Forest classifier finds the compensation table out of 100s of random/noisy tables in the raw DEF14A document
2. **Parse Table Contents**: Messy and inconsistent raw HTML tables get translated into structured JSON objects with Salary details (total, base, bonus, options, etc.)

#### Features
- Can be run locally or on AWS (requires S3, Athena, Lambda)
- Multiprocessing can be toggled on/off via [`config.py`](https://github.com/talsan/ceopay/blob/master/config.py)
- Batch scripts are available to keep it fully synced with Edgar updates (requires S3/Athena configuration)

## Process Architecture
![Process Architecture](https://github.com/talsan/ceopay/blob/master/resources/img/DEF14A%20Data%20Flow.png?raw=true)

## Process Details
### 1. Download Index Files
##### `idx_downloader.py`
##### Inputs: 
```
usage: idx_downloader.py [-h] [--local_output LOCAL_OUTPUT] yyyyqq

downloads individual quarterly master index files from SEC edgar

positional arguments:
  yyyyqq                <yyyyqq> formatted year-quarter pair (eg 202001) to download from edgar

optional arguments:
  -h, --help            show this help message and exit
  --local_output LOCAL_OUTPUT
                        where to send output on local machine; defaults to 's3', which uploads to the config.Aws.OUPUT_BUCKET defined in
                        config.py)
```
##### Outputs:
[Output Example](https://github.com/talsan/ceopay/blob/master/data/masteridx/year%3D2020/qtr%3D2.txt)  
S3 naming convention: `<config.Aws.OUPUT_BUCKET>/masteridx/year=2020/qtr=1.txt`  
Local naming convention: `./ceopay/data/masteridx/year=2020/qtr=1.txt`  

### 2. Extract Filing Header
##### `hdr_extractor.py`
##### Inputs: 
```
usage: hdr_extractor.py [-h] [--local_output LOCAL_OUTPUT] yyyyqq formtype

extract additional metadata (contents within each filing's <SEC-HEADER> tags) across all filings within an individual master index file
(filtered on a given 'formtype')

positional arguments:
  yyyyqq                <yyyyqq> formatted year-quarter pair (eg 202001) to download from edgar
  formtype              eg "DEF 14A", "10-K", "10-Q", etc

optional arguments:
  -h, --help            show this help message and exit
  --local_output LOCAL_OUTPUT
                        where to send output on local machine; defaults to 's3', which uploads to the config.Aws.OUPUT_BUCKET defined in
                        config.py)
```
##### Outputs:
[Output Example](https://github.com/talsan/ceopay/blob/master/data/filing_metadata/formtype%3Ddef14a/year%3D2020/qtr%3D2.txt)  
S3 naming convention: `<config.Aws.OUPUT_BUCKET>/filing_metadata/formtype=def14a/year=2020/qtr=1.txt`  
Local naming convention: `./ceopay/data/filing_metadata/formtype=def14a/year=2020/qtr=1.txt`  
