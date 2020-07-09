# SOURCING & STRUCTURING EXECUTIVE COMPENSATION FROM SEC DISCLOSURES
### Process Overview
##### Generalized for any Edgar Document (10-K, 10-Q, DEF 14A, etc.)
1. **Download Index Files**: process critical lookup index for each Edgar filing (company name, filing date, filing url, etc.) 
2. **Extract Filing Header**: additional filing metadata (contents within filing's `<SEC_HEADER>` tag)
3. **Download Filings**: in this case, the annual proxy filings (`formtype="DEF 14A"`) 

##### Specific to Executive Compensation
1. **Detect Compensation Table**: out of 100s of random/noisy tables in the raw DEF14A document
2. **Parse Table Contents** containing 100s of tables, one of which has executive compensation information into consistently structured json files

### Process Architecture
![Process Architecture](https://github.com/talsan/ceopay/blob/master/resources/img/DEF14A%20Data%20Flow.png?raw=true)

### Process Details
#### 1. Download Index Files
##### `idx_downloader.py`
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
S3 naming convention: `<config.Aws.OUPUT_BUCKET>/masteridx/year=2020/qtr=1.txt`  
Local naming convention: `./ceopay/data/masteridx/year=2020/qtr=1.txt`  
File Contents Sample:
```
fid|Year|Quarter|CIK|CompanyName|FormType|DateFiled|Filename
1000015-0000912057-01-007413|2001|1|1000015|META GROUP INC|8-K|2001-03-09|edgar/data/1000015/0000912057-01-007413.txt
1000015-0001068144-01-000032|2001|1|1000015|META GROUP INC|SC 13G/A|2001-02-02|edgar/data/1000015/0001068144-01-000032.txt
1000015-0000080255-01-000172|2001|1|1000015|META GROUP INC|SC 13G/A|2001-02-08|edgar/data/1000015/0000080255-01-000172.txt
1000015-0000912057-01-004618|2001|1|1000015|META GROUP INC|SC 13G/A|2001-02-12|edgar/data/1000015/0000912057-01-004618.txt
```
##### `batch_idx_downloader.py`
Wrapper around `idx_downloader.py` that keeps your AWS S3 Bucket in Sync w/ Edgar. Script can be run at any time and any frequency (every minute, every week, etc.)
```
usage: batch_idx_downloader.py [-h] [--start START] [--end END] [--local_output LOCAL_OUTPUT] [--overwrite]

batch downloader of quarterly master index files from SEC edgar w/ option to update or overwrite existing downloads

optional arguments:
  -h, --help            show this help message and exit
  --start START         <yyyyqq> format; Start year-qtr of index you wish to download (default=199301)
  --end END             <yyyyqq> format; End year-qtr of index you wish to download (default=present)
  --local_output LOCAL_OUTPUT
                        parent directory of output; if local_output == "s3," it uploads to s3 based on Aws parameters set in config.py
  --overwrite           downloads all requested master idx files (default behavior is to only process those files that aren't yet stored
                        in S3)

```

#### 2. Extract Filing Header
##### `hdr_extractor.py`
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
S3 naming convention: `<config.Aws.OUPUT_BUCKET>/filing_metadata/formtype=def14a/year=2020/qtr=1.txt`  
Local naming convention: `./ceopay/data/filing_metadata/formtype=def14a/year=2020/qtr=1.txt`  
File Contents Sample:
```
fid|acceptancedatetime|accessionnumber|conformedsubmissiontype|publicdocumentcount|conformedperiodofreport|filedasofdate|dateasofchange|effectivenessdate|sicsdesc|sicscode|irsnum|fiscalyearend|stateofincorp
1000209-0001193125-20-123241|2020-04-28 16:50:56|0001193125-20-123241|DEF 14A|5|2020-06-19|2020-04-28|2020-04-28|2020-04-28|FINANCE SERVICES|6199|043291176|1231|DE
1000228-0001193125-20-099923|2020-04-07 07:01:00|0001193125-20-099923|DEF 14A|2|2020-05-21|2020-04-07|2020-04-07|2020-04-07|WHOLESALE-MEDICAL, DENTAL & HOSPITAL EQUIPMENT & SUPPLIES|5047|113136595|1228|DE
1000232-0001558370-20-006159|2020-05-11 12:04:04|0001558370-20-006159|DEF 14A|6|2020-06-16|2020-05-11|2020-05-11|2020-05-11|STATE COMMERCIAL BANKS|6022|610993464|1231|KY
1000298-0001047469-20-002623|2020-04-28 08:30:33|0001047469-20-002623|DEF 14A|4|2020-06-23|2020-04-28|2020-04-28|2020-04-28|REAL ESTATE INVESTMENT TRUSTS|6798|330675505|1231|MD
1000623-0001000623-20-000066|2020-04-10 16:13:24|0001000623-20-000066|DEF 14A|1|2020-04-10|2020-04-10|2020-04-10|2020-04-10|PAPER MILLS|2621|621612879|1231|DE
```
###### `batch_hdr_extractor.py`
Wrapper around `hdr_extractor.py` that keeps filing header metadata in Sync w/ master index files in [1]. Script can be run at any time and any frequency (every minute, every week, etc.)
```
usage: batch_hdr_extractor.py [-h] [--overwrite] [--local_output LOCAL_OUTPUT] formtype

batch extract metadata from filings w/ option to update or overwrite existing extracts in S3

positional arguments:
  formtype              eg "DEF 14A", "10-K", "10-Q", etc

optional arguments:
  -h, --help            show this help message and exit
  --overwrite           overwrite filing headers that have previously been extracted and loaded into S3
  --local_output LOCAL_OUTPUT
                        where to send output on local machine; defaults to 's3', which uploads to the config.Aws.OUPUT_BUCKET defined in
                        config.py)

```
