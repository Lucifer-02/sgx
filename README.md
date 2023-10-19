# SGX Downloader
## Installation

First, install requirements:

```shell
pip3 install -r requirements.txt
```

Then install browser for [Playwright](https://playwright.dev/python/):
```shell
playwright install chromium
```

## Usage

```console
usage: downloader.py [-h] [--config CONFIG] [--output OUTPUT] [--files FILES]
                     [--logfile LOGFILE] [--error-file ERROR_FILE]
                     [--loglevel LOGLEVEL] [--last LAST] [--retry] [--update]
                     [--day DAY] [--start START] [--end END]

SGX derivatives data downloader

options:
  -h, --help            show this help message and exit
  --config CONFIG       Path of config file.
  --output OUTPUT       Directory to save data.
  --files FILES         List of file names to download.
  --logfile LOGFILE     Log file path
  --error-file ERROR_FILE
                        Path of the file that stores the list of failed
                        downloads.
  --loglevel LOGLEVEL   Log level for logging file.
  --last LAST           Download data from the last N days that SGX has data
                        for.
  --retry               Redownload files listed in error log.
  --update              Download the latest data.
  --day DAY             Download data for a specific day. Format: YYYY-MM-DD
  --start START         Start of a date range want to download. Format: YYYY-
                        MM-DD
  --end END             End of a date range want to download. Format: YYYY-MM-
                        DD
```

## Examples

- `python3 downloader.py --update` : download lastest data
- `python3 downloader.py --last 10 --loglevel debug` : download last 10 days available data
- `python3 downloader.py --retry` : redownload error download files
- `python3 downloader.py --day "2023-08-23" --config configs/custom.cfg` : download data on 23/08/2023 with custom config file
- `python3 downloader.py --start "2014-05-28" --end "2014-06-05"` : download data from 28/05/2014 to 05/06/2014 with debug log level

## File structure

```
.
├── configs
│   ├── custom.cfg
│   └── default.cfg
├── database
│   └── db.csv
├── default_errors.csv
├── default.log
├── downloader.py
├── downloads
│   ├── 5492
│   │   ├── TC_20230824.txt
│   │   ├── TC_structure.dat
│   │   ├── TickData_structure.dat
│   │   └── WEBPXTICK_DT-20230824.zip
│   ├── 5493
│   │   ├── TC_20230825.txt
│   │   ├── TC_structure.dat
│   │   ├── TickData_structure.dat
│   │   └── WEBPXTICK_DT-20230825.zip
│   ├── 5494
│   │   ├── TC_20230828.txt
│   │   ├── TC_structure.dat
│   │   ├── TickData_structure.dat
│   │   └── WEBPXTICK_DT-20230828.zip
│   └── 5495
│       ├── TC_20230829.txt
│       ├── TC_structure.dat
│       ├── TickData_structure.dat
│       └── WEBPXTICK_DT-20230829.zip
├── README.pdf
└── requirements.txt
```

## Logging

1. All operation information will be saved to the log file (default is `default.log`) or you can specify with the flag `--logfile` and choose the log level with `--loglevel`.
2. Information about downloaded error files is saved separately (default is `default_errors.csv`) or you can specify with the flag `--error-file`. The file is saved in `csv` format for easy access.

## Recovery

The `--retry` flag will  redownload error files stored in the `default_errors.csv` file by default.


# Designing
## Analysis

After analyzing and exploring download data from the website, I have drawn the following constraints:

- URL formatted download: `https://links.sgx.com/1.0.0/derivatives-historical/<id>/<filename>` where `id` is the unique index of the date and `filename` is the filename to download.
- `id` always increases linearly and does not miss any numbers.
- `id` may not correspond to the date, i.e. there is an `id` but no data to download.
- Dates may be missing.
- Dates may not be sorted ascending by `id`.
- Data is not available on weekends.
- The date is stored in the name of some file of that date.
## Design

<mark>The main purpose is to download data according to the selected date.</mark>

 => From the above analysis, it can be asserted that there is no efficient way to find the correct `id` corresponding to the date to be downloaded just by getting directly from the website because `id` and date are not sorted. by any rule.


>**Idea:** Use an incremental database, i.e. continuously update data about `id` and date respectively. Then retrieve the exact `ids` corresponding to the dates in the database and rely on these `ids` to load the requested data.

The following are the main actions of the tool:

1. Get the latest data
	- Retrieve information about the latest data using the Web Automation tool [Playwright](https://playwright.dev/python/), specifically the latest `id` and date.

2. Request handling
	- Check date format.
	- Check valid date. Ex: 2023-02-30 is invalid.
	- Check the valid time period. Ex: From 2023-08-30 to 2023-08-15 is invalid.
3. Update database
	- With the latest data, we compare with the last data in the database and update the missing data with `Pandas`.
	- Except for the need to download the latest data, the rest need to update the database.
4. Retrieve `id` in the database
	- With input is a day, just query the data according to that date.
	- For a time period, we will get the `id` of the days in that range that are in the database. Invalid days by default do not exist in it.
	- For the last N days, just get the last N records.
5. Download
	- Download modules are designed independently for easy parallelization.
	- Only require `id` and file name.
	- Information about data that cannot be downloaded will be saved to a separate log file for later recovery.

### Design review

1. Advantage:
	- Getting the `id` is quick and easy thanks to centralized database storage.
	- Easily parallelize tasks.
	- Checking valid data is very easy.

2. Defect:
	- Need to create initial database.
	- If not operated regularly, the database update time will be large.

### Program problems

1. Parallelization of new tasks has not been as effective as expected, most likely due to the influence of IO.
2. Balancing Performance, Readability and Decoupling remains a challenge.
3. Using global variables reduces modularity.
