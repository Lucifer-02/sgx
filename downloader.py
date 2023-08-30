from urllib.request import urlopen, urlretrieve
from urllib.error import HTTPError, URLError, ContentTooShortError
from datetime import datetime, timedelta
import cgi
import logging
import re
import os
import argparse
import configparser
import sys
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from playwright.sync_api import Playwright, sync_playwright


def _log_init(xargs: argparse.Namespace):
    root_logger = logging.getLogger()
    root_logger.setLevel(xargs.loglevel.upper())
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    stream_handler.setLevel(xargs.loglevel.upper())
    file_handle = logging.FileHandler(filename=xargs.logfile, encoding="utf8")
    file_handle.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    file_handle.setLevel(xargs.loglevel.upper())

    failed = logging.getLogger("failed")
    failed.setLevel(logging.WARNING)
    failed_file = logging.FileHandler(filename=xargs.error_file, encoding="utf8")

    failed.addHandler(failed_file)
    root_logger.addHandler(file_handle)
    root_logger.addHandler(stream_handler)


def _extract_id_from_url(url: str) -> int:
    return int(re.findall(r"\d+", url)[-1])


def _extract_date_from_filename(filename: str) -> datetime:
    str_day = "".join(re.findall(r"\d+", filename))
    return datetime.strptime(str_day, "%Y%m%d")


def _id_to_url(date_id: int, filename: str) -> str:
    return f"{URL_PATTERN}/{date_id}/{filename}"


def _get_date_from_id(date_id: int) -> datetime | None:
    dummy_filename = "WEBPXTICK_DT.zip"
    url = _id_to_url(date_id, dummy_filename)

    with urlopen(url=url) as response:
        content_disposition = response.headers.get("Content-Disposition")
        if content_disposition is not None:
            _, params = cgi.parse_header(content_disposition)
            filename = params["filename"]
            return _extract_date_from_filename(filename)

    logging.warning("Content disposition is None. Not found '%s'.", url)
    logging.getLogger("failed").error("'%s'\tFileNotFoundError", url)

    return None


def _get_id_from_date(date: datetime) -> int | None:
    index_table = _update_db()
    return index_table[index_table["date"] == date]["date_id"].values[0]


def _download_file(url: str, save_dir: str, timeout=3) -> str | None:
    logging.debug("Downloading %s", url)
    downloaded_file = None
    with urlopen(url=url, timeout=timeout) as file_info:
        content_disposition = file_info.info()["Content-Disposition"]
        if content_disposition is not None:
            _, params = cgi.parse_header(content_disposition)
            downloaded_file = params["filename"]
            urlretrieve(url=url, filename=save_dir + "/" + downloaded_file)
            return downloaded_file

        logging.warning("Content disposition is None. Download failed.")

        return None


def _get_file_by_id(request_file: str, save_dir: str, date_id: int) -> bool:
    success = True
    downloaded_file = None
    failed_log = logging.getLogger("failed")

    url = _id_to_url(date_id, request_file)

    try:
        downloaded_file = _download_file(url, save_dir)
        success = downloaded_file is not None

    except HTTPError as he:
        logging.error("HTTPError: %s", he.reason)
        failed_log.error("%s,%s,HTTPError,%s", date_id, request_file, he.reason)
        success = False
    except FileNotFoundError as fnfe:
        logging.error("FileNotFoundError: %s", fnfe.strerror)
        failed_log.error(
            "%s,%s,FileNotFoundError,%s", date_id, request_file, fnfe.strerror
        )
        success = False
    except ContentTooShortError as ctse:
        logging.error("ContentTooShortError: %s", ctse.reason)
        failed_log.error(
            "%s,%s,ContentTooShortError,%s", date_id, request_file, ctse.reason
        )
        success = False
    except URLError as ue:
        logging.error("URLError: %s", ue.reason)
        failed_log.error("%s,%s,URLError,%s", date_id, request_file, ue.reason)
        success = False
    except OSError as oe:
        logging.error("OSError: %s", oe.strerror)
        failed_log.error("%s,%s,OSError,%s", date_id, request_file, oe.strerror)

    if success:
        logging.info("Downloaded %s to %s/", downloaded_file, save_dir)

    return success


def _get_files_by_date(
    request_files: list[str],
    save_dir: str,
    request_date: datetime,
) -> int:
    if _is_weekend(request_date) or _is_future(request_date, LASTEST_DATE):
        return -1

    date_id = _get_id_from_date(date=request_date)
    if date_id is None:
        logging.warning("Date id of %s not found.", request_date.strftime("%Y-%m-%d"))
        return -1

    return _get_files_by_id(request_files, save_dir, date_id)


def _get_valid_dates(start_date: datetime, end_date: datetime) -> list[datetime]:
    dates = []
    date = start_date
    while date <= end_date:
        if not _is_weekend(date) and not _is_future(date, LASTEST_DATE):
            dates.append(date)
        else:
            logging.debug("Skip %s", date.strftime("%Y-%m-%d"))
        date += timedelta(days=1)
    return dates


def _get_ids_from_dates(dates: list[datetime]) -> list[int]:
    index_table = _update_db()
    date_ids = index_table[index_table["date"].isin(dates)]["date_id"]
    return date_ids.tolist()


def _get_least_ids(number: int) -> list[int]:
    index_table = _update_db()
    date_ids = index_table["date_id"].tail(number)
    return date_ids.tolist()


def _is_valid_range(start_date: datetime, end_date: datetime) -> bool:
    if start_date > end_date:
        logging.warning("Start date is later than End date.")
        return False
    return True


def _is_weekend(date: datetime) -> bool:
    if date.weekday() in [5, 6]:
        logging.debug("%s is weekend.", date.strftime("%Y-%m-%d"))
        return True
    return False


def _get_lastest_info(playwright: Playwright) -> tuple[int, datetime]:
    logging.debug("Getting lastest date...")
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    logging.debug("Navigating to SGX website...")
    page = context.new_page()
    page.goto("https://www.sgx.com/research-education/derivatives")
    with page.expect_download() as download_info:
        page.locator(
            "widget-reports-derivatives-tick-and-trade-cancellation"
        ).get_by_role("button", name="Download").click()

    logging.debug("Getting download info")

    download = download_info.value
    url = download.url
    filename = download.suggested_filename

    date_id = _extract_id_from_url(url)
    date = _extract_date_from_filename(filename)

    # ---------------------
    context.close()
    browser.close()
    return date_id, date


def _is_future(date: datetime, current_date: datetime) -> bool:
    if date > current_date:
        logging.warning("%s is not in the historical.", date)
        return True
    return False


def _check_valid_date(date_str: str) -> datetime | None:
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d")
        return date
    except ValueError:
        logging.error("%s Date Format Error!", date_str)

    return None


def _update_db() -> pd.DataFrame:
    logging.info("Reading database...")

    db = pd.read_csv(DATABASE_PATH, parse_dates=["date"])
    db_lastest_date = db["date"].max()
    db_lastest_id = db["date_id"].max()

    if db_lastest_date == LASTEST_DATE:
        logging.info("Database is up to date.")
        return db

    logging.info("Updating database...")
    date_id = db_lastest_id + 1
    appends = []

    while date_id <= LASTEST_ID:
        try:
            date = _get_date_from_id(date_id)
            if date is not None:
                appends.append({"date_id": date_id, "date": date})

        except HTTPError as http_error:
            logging.error("Get date from id %d failed.", date_id)
            logging.error("HTTPError: %s", http_error.reason)
        date_id += 1

    # concat new data to db
    db = pd.concat([db, pd.DataFrame(appends)], ignore_index=True)
    db.to_csv(DATABASE_PATH, index=False)
    logging.info("Database updated.")
    return db


def _get_files_by_id(request_files: list[str], save_dir: str, date_id: int) -> int:
    sub_dir = f"{save_dir}/{date_id}"
    if not os.path.exists(sub_dir):
        os.mkdir(sub_dir)

    states = []

    # for request_file in request_files:
    #     status = _get_file_by_id(request_file, sub_dir, date_id)
    #     states.append(status)

    # using multi-processing
    with Pool(processes=12) as pool:
        states = pool.starmap(
            _get_file_by_id,
            [(request_file, sub_dir, date_id) for request_file in request_files],
        )

    # using multi-threading
    # with ThreadPoolExecutor(max_workers=12) as executor:
    #     futures = [
    #         executor.submit(_get_file_by_id, request_file, sub_dir, date_id)
    #         for request_file in request_files
    #     ]
    #     for future in as_completed(futures):
    #         future.result()

    # count number of errors
    return len(states) - sum(states)


def _get_files_by_ids(
    request_files: list[str], save_dir: str, date_ids: list[int]
) -> int:
    error_count = 0
    for date_id in date_ids:
        num_errors = _get_files_by_id(request_files, save_dir, date_id)
        error_count += num_errors

    return error_count


def _get_files_by_dates(
    request_files: list[str], save_dir: str, dates: list[datetime]
) -> int:
    date_ids = _get_ids_from_dates(dates)
    return _get_files_by_ids(request_files, save_dir, date_ids)


def get_files_by_date_str(request_files: list[str], save_dir: str, request_date: str):
    """Download files for a specific date."""
    date = _check_valid_date(request_date)
    if date is None:
        logging.warning("Invalid date: %s, skipping", request_date)
        return

    logging.info("Downloading files for %s...", date)
    num_errors = _get_files_by_date(request_files, save_dir, date)

    if num_errors != -1:
        logging.info(
            "Finished downloading files for %s. %d errors.",
            date.strftime("%Y-%m-%d"),
            num_errors,
        )


def get_last_files(request_files: list[str], save_dir: str, days: int):
    """Download files in the last N days."""

    date_ids = _get_least_ids(days)
    num_errors = _get_files_by_ids(request_files, save_dir, date_ids)

    logging.info("Finished downloading lastest %d days. %d errors.", days, num_errors)


def get_lastest_files(request_files: list[str], save_dir: str):
    """Download files in the lastest date."""
    num_errors = _get_files_by_date(request_files, save_dir, LASTEST_DATE)

    logging.info(
        "Finished downloading lastest files on %s. %d errors.",
        LASTEST_DATE.strftime("%Y-%m-%d"),
        num_errors,
    )


def get_range_files(
    request_files: list[str],
    save_dir: str,
    start_date: str,
    end_date: str,
):
    """Download files in a range of dates."""

    from_date = _check_valid_date(start_date)
    to_date = _check_valid_date(end_date)

    if (
        (from_date is None)
        or (to_date is None)
        or (not _is_valid_range(from_date, to_date))
    ):
        return

    valid_dates = _get_valid_dates(from_date, to_date)
    num_errors = _get_files_by_dates(request_files, save_dir, valid_dates)

    logging.info(
        "Finished downloading files from %s to %s. %d errors.",
        start_date,
        end_date,
        num_errors,
    )


def retry_download_errors(errors_file, save_dir):
    """Retry download errors files in error log file."""

    logging.info("Retrying download errors...")

    # check empty error log
    if os.stat(errors_file).st_size == 0:
        logging.info("No errors.")
        return

    errors = pd.read_csv(errors_file, header=None)
    errors = errors.drop_duplicates()

    for _, row in errors.iterrows():
        date_id = row[0]
        request_file = row[1]
        status = _get_file_by_id(
            request_file=request_file,
            # save_dir=save_dir,
            save_dir=save_dir + "/" + str(date_id),
            date_id=date_id,
        )
        if not status:
            logging.error("Download %s of id %d failed.", request_file, date_id)
            continue

        # remove error from error log
        errors = errors[~((errors[0] == date_id) & (errors[1] == request_file))]

    errors.to_csv(errors_file, index=False, header=False)


def get_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def apply_config(xargs: argparse.Namespace) -> argparse.Namespace | None:
    """Apply config to args."""

    global URL_PATTERN, DATABASE_PATH

    if not os.path.exists(xargs.config):
        logging.error("Config file is not exists!")
        return None

    config = get_config(xargs.config)

    URL_PATTERN = config.get("BASE", "URL_PATTERN")
    DATABASE_PATH = config.get("BASE", "database")

    xargs.dateformat = config.get("BASE", "dateformat")
    xargs.output = config.get("BASE", "output")
    xargs.logfile = config.get("BASE", "logfile")
    xargs.error_file = config.get("BASE", "error_file")
    xargs.loglevel = config.get("BASE", "loglevel")
    xargs.files = config.get("BASE", "download_files")

    return xargs


LASTEST_ID: int
LASTEST_DATE: datetime


def run(xargs: argparse.Namespace):
    global LASTEST_ID, LASTEST_DATE

    _log_init(xargs)
    logging.info("--------------------" * 3)
    logging.info("Starting...")

    # get lastest info from SGX website
    # with sync_playwright() as playwright:
    #     LASTEST_ID, LASTEST_DATE = _get_lastest_info(playwright)
    #     logging.info("Lastest date: %s", LASTEST_DATE.strftime("%Y-%m-%d"))

    LASTEST_ID = 5495
    LASTEST_DATE = datetime(2023, 8, 29)

    if not (xargs.update or xargs.day or xargs.start or xargs.retry or xargs.last):
        logging.info("Please choose at least an action!")
        sys.exit()

    if not os.path.exists(xargs.output):
        os.mkdir(xargs.output)

    files = xargs.files.split(",")

    if xargs.update:
        get_lastest_files(files, xargs.output)

    if xargs.day:
        get_files_by_date_str(files, xargs.output, xargs.day)

    if xargs.start and xargs.end:
        get_range_files(files, xargs.output, xargs.start, xargs.end)

    if xargs.last and xargs.last > 0:
        get_last_files(files, xargs.output, xargs.last)

    if xargs.retry:
        retry_download_errors(xargs.error_file, xargs.output)

    logging.info("Done.")


URL_PATTERN: str
DATABASE_PATH: str

if __name__ == "__main__":
    default_config = get_config("configs/default.cfg")
    parser = argparse.ArgumentParser(description="SGX derivatives data downloader")
    parser.add_argument(
        "--config",
        type=str,
        help="Path of config file.",
    )

    # -------------------- Configs --------------------
    parser.add_argument(
        "--output",
        type=str,
        help="Directory to save data.",
        default=default_config.get("BASE", "output"),
    )
    parser.add_argument(
        "--files",
        type=str,
        help="List of file names to download.",
        default=default_config.get("BASE", "download_files"),
    )
    parser.add_argument(
        "--logfile",
        type=str,
        help="Log file path",
        default=default_config.get("BASE", "logfile"),
    )

    parser.add_argument(
        "--error-file",
        type=str,
        help="Path of the file that stores the list of failed downloads.",
        default=default_config.get("BASE", "error_file"),
    )
    parser.add_argument(
        "--loglevel",
        type=str,
        help="Log level for logging file.",
        default=default_config.get("BASE", "loglevel"),
    )

    # -------------------- Actions --------------------
    parser.add_argument(
        "--last",
        type=int,
        help="Download data from the last N days that SGX has data for.",
    )
    parser.add_argument(
        "--retry",
        action="store_true",
        help="Redownload files listed in error log.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Download the latest data.",
    )
    parser.add_argument(
        "--day",
        type=str,
        help="Download data for a specific day. Format: YYYY-MM-DD",
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Start of a date range want to download. Format: YYYY-MM-DD",
        required="--end" in sys.argv,
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End of a date range want to download. Format: YYYY-MM-DD",
        required="--start" in sys.argv,
    )

    args = parser.parse_args()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    if args.config is not None:
        args = apply_config(args)
        if args is None:
            sys.exit()
    else:
        URL_PATTERN = default_config.get("BASE", "URL_PATTERN")
        DATABASE_PATH = default_config.get("BASE", "database")

    run(args)
