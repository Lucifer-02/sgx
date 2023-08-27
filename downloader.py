import re
from urllib.request import urlopen, urlretrieve
from urllib.error import HTTPError, URLError, ContentTooShortError
import cgi
from datetime import datetime
from playwright.sync_api import Playwright, sync_playwright
import logging
import pandas as pd
import os


def _log_init():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler("downloader.log"),
            logging.StreamHandler(),
        ],
    )


def _extract_id_from_url(url: str) -> int:
    return int(re.findall(r"\d+", url)[-1])


def _extract_date_from_filename(filename: str) -> datetime:
    str_day = "".join(re.findall(r"\d+", filename))
    return datetime.strptime(str_day, "%Y%m%d")


def _id_to_url(date_id: int, filename: str) -> str:
    return f"https://links.sgx.com/1.0.0/derivatives-historical/{date_id}/{filename}"


def _get_date_from_id(date_id: int) -> datetime | None:
    dummy_filename = "WEBPXTICK_DT_20210831.zip"
    url = _id_to_url(date_id, dummy_filename)

    response = urlopen(url=url)
    content_disposition = response.headers.get("Content-Disposition")
    if content_disposition is not None:
        _, params = cgi.parse_header(content_disposition)
        filename = params["filename"]
        return _extract_date_from_filename(filename)
    else:
        logging.warning(f"Content disposition is None. Not found '{url}'.")
        logging.getLogger("failed").error(f"{url}\tFileNotFoundError")
    return None


def _get_id_from_date(date: datetime, db: pd.DataFrame) -> int | None:
    db = _update_db(db)
    return db[db["date"] == date]["date_id"].values[0]


def _download_file(url: str, save_dir: str, timeout=3) -> str | None:
    logging.info(f"Downloading {url}")
    file_info = urlopen(url=url, timeout=timeout)
    content_disposition = file_info.info()["Content-Disposition"]
    if content_disposition is not None:
        _, params = cgi.parse_header(content_disposition)
        downloaded_file = params["filename"]
        urlretrieve(
            url=url,
            filename=save_dir + "/" + downloaded_file,
        )
    else:
        logging.warning(
            f"Content disposition is None. Not found '{url}'. Download failed."
        )
        logging.getLogger("failed").error(f"{url}\tFileNotFoundError")
        return None

    return downloaded_file


def _get_file_by_id(request_file: str, save_dir: str, date_id: int) -> bool:
    success = False
    downloaded_file = None

    url = _id_to_url(date_id, request_file)
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
    try:
        downloaded_file = _download_file(url, save_dir)
        success = False if downloaded_file is None else True

    except HTTPError as he:
        logging.error(f"HTTPError: {he}")
        success = False
    except FileNotFoundError as fnfe:
        logging.error(f"FileNotFoundError: {fnfe}")
        success = False
    except ContentTooShortError as ctse:
        logging.error(f"ContentTooShortError: {ctse}")
        success = False
    except URLError as ue:
        logging.error(f"URLError: {ue}")
        success = False
    except OSError as oe:
        logging.error(f"OSError: {oe}")

    if success:
        logging.info(f"Downloaded {downloaded_file}")
        pass

    return success


def _get_file_by_date(
    request_files: list[str], save_dir: str, request_date: datetime, db: pd.DataFrame
) -> list[tuple[int, str]]:
    logging.info(f"Getting file of {request_date}...")

    errors = []

    if _is_weekend(request_date) or _is_future(request_date):
        return []

    date_id = _get_id_from_date(date=request_date, db=db)
    if date_id is None:
        logging.warning(f"Date id of {request_date} not found.")
        return []

    for request_file in request_files:
        status = _get_file_by_id(request_file, save_dir, date_id)
        if not status:
            errors.append((date_id, request_file))

    return errors


def _get_ids_from_dates(
    start_date: datetime, end_date: datetime, db: pd.DataFrame
) -> list[int]:
    db = _update_db(db)
    date_ids = db[(db["date"] >= start_date) & (db["date"] <= end_date)]["date_id"]
    return date_ids.tolist()


def _get_least_ids(number: int, db: pd.DataFrame) -> list[int]:
    db = _update_db(db)
    date_ids = db["date_id"].tail(number)
    return date_ids.tolist()


def _is_valid_range(start_date: str, end_date: str) -> bool:
    if start_date > end_date:
        logging.warning(f"Start date is later than end date.")
        return False
    return True


def _is_weekend(date: datetime) -> bool:
    if date.weekday() in [5, 6]:
        logging.warning(f"{date} is weekend.")
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
    except ValueError as ve:
        logging.error(f"ValueError: {ve}")

    return None


def _update_db(db: pd.DataFrame) -> pd.DataFrame:
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

        except HTTPError as he:
            logging.error(f"Get date from id {date_id} failed.")
            logging.error(f"HTTPError: {he}")
        date_id += 1

    # concat new data to db
    db = pd.concat([db, pd.DataFrame(appends)], ignore_index=True)
    db.to_csv("db.csv", index=False)
    logging.info("Database updated.")
    return db


def _save_errors(errors: list[tuple[int, str]], errors_file="errors.csv"):
    if len(errors) == 0:
        logging.info("No errors.")
        return
    with open(errors_file, "w") as f:
        for error in errors:
            f.write(f"{error[0]},{error[1]}\n")
    logging.info(f"Errors saved to {errors_file}.")


def get_file_by_date_str(
    request_files: list[str], save_dir: str, request_date: str, db: pd.DataFrame
) -> list[tuple[int, str]]:
    date = _check_valid_date(request_date)
    if date is None:
        logging.warning(f"Invalid date: {request_date}, skipping")
        return []

    return _get_file_by_date(request_files, save_dir, date, db)


def get_last_files(
    request_files: list[str], save_dir: str, days: int, db: pd.DataFrame
) -> list[tuple[int, str]]:
    date_ids = _get_least_ids(days, db)
    errors = []

    for date_id in date_ids:
        for request_file in request_files:
            status = _get_file_by_id(request_file, save_dir, date_id)
            if not status:
                errors.append((date_id, request_file))

    return errors


def get_lastest_files(request_files: list[str], save_dir: str) -> list[tuple[int, str]]:
    errors = []
    for request_file in request_files:
        status = _get_file_by_id(request_file, save_dir, LASTEST_ID)
        if not status:
            errors.append((LASTEST_ID, request_file))

    return errors


def get_range_files(
    request_files: list[str],
    save_dir: str,
    start_date: str,
    end_date: str,
    db: pd.DataFrame,
) -> list[tuple[int, str]]:
    start = _check_valid_date(start_date)
    end = _check_valid_date(end_date)

    if start is None or end is None or not _is_valid_range(start_date, end_date):
        return []

    date_ids = _get_ids_from_dates(start, end, db)
    errors = []

    for date_id in date_ids:
        for request_file in request_files:
            status = _get_file_by_id(request_file, save_dir, date_id)
            if not status:
                errors.append(date_id)

    return errors


def retry_download_errors(errors_file="errors.csv", save_dir="downloads"):
    logging.info("Retrying download errors...")
    # check empty errors file
    if os.stat(errors_file).st_size == 0:
        logging.info("No errors.")
        return
    errors = pd.read_csv(errors_file, header=None)
    new_errors = []
    for _, row in errors.iterrows():
        date_id = row[0]
        request_file = row[1]
        status = _get_file_by_id(request_file, save_dir, date_id)
        if not status:
            new_errors.append((date_id, request_file))
            logging.error(f"Download {request_file} of {date_id} failed.")

    _save_errors(new_errors)


def get_lastest_info(playwright: Playwright) -> tuple[int, datetime]:
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    page.goto("https://www.sgx.com/research-education/derivatives")
    with page.expect_download() as download_info:
        page.locator(
            "widget-reports-derivatives-tick-and-trade-cancellation"
        ).get_by_role("button", name="Download").click()
    download = download_info.value
    url = download.url
    filename = download.suggested_filename

    date_id = _extract_id_from_url(url)
    date = _extract_date_from_filename(filename)

    # ---------------------
    context.close()
    browser.close()
    return date_id, date


if __name__ == "__main__":
    _log_init()

    logging.info("---------------------------------------------")
    logging.info("Starting...")
    with sync_playwright() as playwright:
        logging.debug("Getting lastest date...")
        LASTEST_ID, LASTEST_DATE = get_lastest_info(playwright)
        logging.info(f"Lastest date: {LASTEST_DATE}")

    # LASTEST_ID = 5492
    # LASTEST_DATE = datetime(2023, 8, 24)

    logging.info("Reading database...")
    db = pd.read_csv("db.csv", parse_dates=["date"])

    # result = get_lastest_file(["WEBPXTICK_DT.zip"], "downloads")

    # get_file_by_date("WEBPXTICK_DT.zip", "downloads", "2023-08-31", db)
    request_files = [
        "WEBPXTICK_DT.zip",
        "TC.txt",
        "TickData_structure.dat",
        "TC_structure.dat",
    ]
    errors = get_range_files(
        request_files=request_files,
        save_dir="downloads",
        start_date="2023-08-12",
        end_date="2023-08-21",
        db=db,
    )
    # errors = get_last_files(
    #     request_files=request_files,
    #     save_dir="downloads",
    #     days=5,
    #     db=db,
    # )
    _save_errors(errors)

    # retry_download_errors()
