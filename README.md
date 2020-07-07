# SOURCING & STRUCTURING EXECUTIVE COMPENSATION FROM SEC DISCLOSURES
### Process Overview
1. **Download Index Files**: critical metadata for each Edgar filing (company name, filing date, filing url, etc.) 
2. **Extract Filing Header**: additional metadata *inside* each filing
3. **Download Proxy Disclosures (DEF 14A)**: annual proxy filings containing 100s of tables, one of which has executive compensation information
4. **Detect Compensation Table**: annual salary for top company executives (including base, bonus, options, etc.)
5. **Parse Table Contents** into consistently structured json files

### Process Architecture
![Process Architecture](https://github.com/talsan/ceopay/blob/master/resources/img/DEF14A%20Data%20Flow.png?raw=true)

### Process Details
#### 1. Download Index Files
##### `invoke_idx_downloader.py`
Creates a queue of `unprocessed_index_files` -- index files (stored on a quarterly basis) which have yet to be downloaded from Edgar. If `overwrite=True`, it processes every available index file after a given `startdate`. After queuing a (quarterly) list of index_files, it invokes `idx_downloader.py`. Note the current quarter is always reprocessed.

##### `idx_downloader.py`
Downloads individual quarterly index files and stores them in S3. If, `output_to_local=True`, it outputs the result to a identically structured local folder heirarchy in `./data/masteridx/..`
S3 Naming Example: `../masteridx/year=2020/qtr=4/idx.csv`

#### 2. Extract Filing Header
##### `invoke_hdr_extractor.py`
Creates a queue of `unprocessed_headers` by identifying the quarterly index files which do not yet have accompanying quarterly hdr/metadata files. If `overwrite=True`, it quees every downloaded index file. After queuing this quarterly list, it invokes `hdr_extractor.py`. Note, the current quarter is always reprocessed.

###### `hdr_extractor.py`
Loops through each filing (i.e. each row) in a given quarterly index file and extracts its header information. Header data from all filings are combined into a single quarterly csv file in S3. If, `output_to_local=True`, it outputs the result to a identically structured local folder hierarchy in `./data/filing_metadata/..`
S3 Naming Example: `../masteridx/year=2020/qtr=4/idx.csv`
