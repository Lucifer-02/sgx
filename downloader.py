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

import pandas as pd
from playwright.sync_api import Playwright, sync_playwright


def _log_init(xargs: argparse.Namespace):
    """Initiate the root logger and an error-only logger."""
    root_logger = logging.getLogger()
    root_logger.setLevel(xargs.loglevel.upper())
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(name)s] %(levelname)s %(message)s")
    )
    stream_handler.setLevel(xargs.loglevel.upper())
    file_handle = logging.FileHandler(filename=xargs.logfile, encoding="utf8")
    file_handle.setFormatter(
        logging.Formatter("%(asctime)s [%(name)s] %(levelname)s %(message)s")
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

    logging.warning(f"Content disposition is None. Not found '{url}'.")
    logging.getLogger("failed").error(f"'{url}'\tFileNotFoundError")

    return None


def _get_id_from_date(date: datetime) -> int | None:
    index_table = _update_db()
    return index_table[index_table["date"] == date]["date_id"].values[0]


def _download_file(url: str, save_dir: str, timeout=3) -> str | None:
    logging.debug(f"Downloading {url}")
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
    sub_dir = f"{save_dir}/{date_id}"
    if not os.path.exists(sub_dir):
        os.mkdir(sub_dir)

    try:
        downloaded_file = _download_file(url, sub_dir)
        success = downloaded_file is not None

    except HTTPError as he:
        logging.error(f"HTTPError: {he}")
        failed_log.error(f"{date_id},{request_file},HTTPError,{he.reason}")
        success = False
    except FileNotFoundError as fnfe:
        logging.error(f"FileNotFoundError: {fnfe.strerror}")
        failed_log.error(f"{date_id},{request_file},FileNotFoundError,{fnfe.strerror}")
        success = False
    except ContentTooShortError as ctse:
        logging.error(f"ContentTooShortError: {ctse.reason}")
        failed_log.error(f"{date_id},{request_file},ContentTooShortError,{ctse.reason}")
        success = False
    except URLError as ue:
        logging.error(f"URLError: {ue.reason}")
        failed_log.error(f"{date_id},{request_file},URLError,{ue.reason}")
        success = False
    except OSError as oe:
        logging.error(f"OSError: {oe.strerror}")
        failed_log.error(f"{date_id},{request_file},OSError,{oe.strerror}")

    if success:
        logging.info(f"Downloaded {downloaded_file} to {sub_dir}/")

    return success


def _get_file_by_date(
    request_files: list[str],
    save_dir: str,
    request_date: datetime,
) -> list[tuple[int, str]]:
    errors = []

    if _is_weekend(request_date) or _is_future(request_date):
        return []

    date_id = _get_id_from_date(date=request_date)
    if date_id is None:
        logging.warning("Date id of {request_date} not found.")
        return []

    for request_file in request_files:
        status = _get_file_by_id(
            request_file=request_file,
            save_dir=save_dir,
            date_id=date_id,
        )
        if not status:
            errors.append((date_id, request_file))

    return errors


def get_valid_dates(start_date: datetime, end_date: datetime) -> list[datetime]:
    dates = []
    date = start_date
    while date <= end_date:
        if not _is_weekend(date) and not _is_future(date):
            dates.append(date)
        else:
            logging.debug("Skip {date}")
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
        logging.debug(f"{date} is weekend.")
        return True
    return False


def _is_future(date: datetime) -> bool:
    if date > LASTEST_DATE:
        logging.warning(f"{date} is not in the historical.")
        return True
    return False


def _check_valid_date(date_str: str) -> datetime | None:
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d")
        return date
    except ValueError:
        logging.error(f"{date_str} Date Format Error!")

    return None


def _update_db() -> pd.DataFrame:
    logging.info("Reading database...")

    db = pd.read_csv("database/db.csv", parse_dates=["date"])
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
            logging.error(f"Get date from id {date_id} failed.")
            logging.error(f"HTTPError: {http_error}")
        date_id += 1

    # concat new data to db
    db = pd.concat([db, pd.DataFrame(appends)], ignore_index=True)
    db.to_csv("database/db.csv", index=False)
    logging.info("Database updated.")
    return db


def get_files_by_date_str(request_files: list[str], save_dir: str, request_date: str):
    date = _check_valid_date(request_date)
    if date is None:
        logging.warning(f"Invalid date: {request_date}, skipping")
        return

    logging.info(f"Downloading files for {date}...")
    _get_file_by_date(request_files, save_dir, date)


def get_last_files(request_files: list[str], save_dir: str, days: int):
    date_ids = _get_least_ids(days)

    for date_id in date_ids:
        for request_file in request_files:
            _get_file_by_id(
                request_file=request_file,
                save_dir=save_dir,
                date_id=date_id,
            )

    logging.info(f"Finished downloading lastest {days} days.")


def get_lastest_files(request_files: list[str], save_dir: str):
    for request_file in request_files:
        _get_file_by_id(
            request_file=request_file,
            save_dir=save_dir,
            date_id=LASTEST_ID,
        )

    logging.info(f"Finished downloading lastest files from {LASTEST_DATE}.")


def get_range_files(
    request_files: list[str],
    save_dir: str,
    start_date: str,
    end_date: str,
):
    from_date = _check_valid_date(start_date)
    to_date = _check_valid_date(end_date)

    if (
        (from_date is None)
        or (to_date is None)
        or (not _is_valid_range(from_date, to_date))
    ):
        return

    valid_dates = get_valid_dates(from_date, to_date)
    date_ids = _get_ids_from_dates(valid_dates)

    for date_id in date_ids:
        for request_file in request_files:
            _get_file_by_id(
                request_file=request_file,
                save_dir=save_dir,
                date_id=date_id,
            )

    logging.info(f"Finished downloading files from {start_date} to {end_date}.")


def retry_download_errors(errors_file="errors.csv", save_dir="downloads"):
    logging.info("Retrying download errors...")

    # check empty errors file
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
            save_dir=save_dir,
            date_id=date_id,
        )
        if not status:
            logging.error("Download %s of %d failed.", request_file, date_id)

        # remove error
        errors = errors[~((errors[0] == date_id) & (errors[1] == request_file))]

    errors.to_csv(errors_file, index=False, header=False)


def get_lastest_info(playwright: Playwright) -> tuple[int, datetime]:
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


def _apply_config(xargs: argparse.Namespace) -> argparse.Namespace:
    global URL_PATTERN

    config = configparser.ConfigParser()
    config.read(xargs.config)

    URL_PATTERN = config.get("BASE", "URL_PATTERN")
    xargs.dateformat = config.get("BASE", "dateformat")
    xargs.output = config.get("BASE", "output")
    xargs.logfile = config.get("BASE", "logfile")
    xargs.error = config.get("BASE", "errorfile")
    xargs.loglevel = config.get("BASE", "loglevel")
    xargs.files = config.get("BASE", "downloadfiles")

    return xargs


def run(xargs: argparse.Namespace):
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


def _get_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


if __name__ == "__main__":
    default_config = _get_config("configs/default_config.ini")
    parser = argparse.ArgumentParser(description="SGX derivatives data downloader")
    parser.add_argument(
        "--config",
        type=str,
        help="Path of config file.",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Directory to save data.",
        default=default_config.get("BASE", "output"),
    )
    parser.add_argument(
        "--files",
        type=str,
        help="List of files to download.",
        default=default_config.get("BASE", "downloadfiles"),
    )
    parser.add_argument(
        "--logfile",
        type=str,
        help="Log file path, default is downloader.log.",
        default=default_config.get("BASE", "logfile"),
    )

    parser.add_argument(
        "--error-file",
        type=str,
        help="Path to the file that stores the list of failed downloads.",
        default=default_config.get("BASE", "errorfile"),
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
        help="Redownload files listed in failed download log.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Download the latest data.",
    )
    parser.add_argument(
        "--day",
        type=str,
        help="Download data for a specific day.",
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Start date of a range download job.",
        required="--end" in sys.argv,
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End date of a range download job.",
        required="--start" in sys.argv,
    )

    args = parser.parse_args()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    if args.config is not None:
        args = _apply_config(args)
    else:
        URL_PATTERN = default_config.get("BASE", "URL_PATTERN")

    _log_init(args)
    logging.info("--------------------" * 3)
    logging.info("Starting...")
    playwright = sync_playwright().start()
    LASTEST_ID, LASTEST_DATE = get_lastest_info(playwright)
    logging.info(f"Lastest date: {LASTEST_DATE}")

    run(args)
