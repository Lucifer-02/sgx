import re
from urllib.request import urlopen, urlretrieve
from urllib.error import HTTPError, URLError, ContentTooShortError
import cgi
from datetime import date, timedelta
from playwright.sync_api import Playwright, sync_playwright


def lastest_info(playwright: Playwright) -> tuple[int, date]:
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

    id = extract_id_from_url(url)
    date = extract_date_from_filename(filename)

    # ---------------------
    context.close()
    browser.close()
    return id, date


def extract_id_from_url(url: str) -> int:
    return int(re.findall(r"\d+", url)[-1])


def extract_date_from_filename(filename: str) -> date:
    str_day = "".join(re.findall(r"\d+", filename))
    return date(int(str_day[:4]), int(str_day[4:6]), int(str_day[6:]))


def id_to_url(id: int, filename: str) -> str:
    return "https://links.sgx.com/1.0.0/derivatives-historical/{id}/{fileName}".format(
        id=id, fileName=filename
    )


def get_date_from_id(date_id: int) -> date | None:
    url = "https://links.sgx.com/1.0.0/derivatives-historical/{date_id}/{fileName}".format(
        date_id=date_id,
        fileName="WEBPXTICK_DT.zip",
    )

    response = urlopen(url=url)
    content_disposition = response.headers.get("Content-Disposition")
    if content_disposition is not None:
        _, params = cgi.parse_header(content_disposition)
        filename = params["filename"]
        return extract_date_from_filename(filename)
    return None


def search_id_with_date(date: date, max_id: int, algorithm: str) -> int | None:
    if algorithm == "linear":
        # reverse linear search
        for id in range(max_id, 0, -1):
            dateInfo = get_date_from_id(id)
            if dateInfo is not None and dateInfo == date:
                return id

    if algorithm == "binary":
        left = 0
        right = max_id
        while left <= right:
            mid = (left + right) // 2
            dateInfo = get_date_from_id(mid)
            if dateInfo is not None and dateInfo == date:
                return mid
            elif dateInfo is not None and dateInfo < date:
                left = mid + 1
            else:
                right = mid - 1

    return None


# get the nearest valid date
def valid_date(date_id: int) -> tuple[int, date]:
    dateInfo = get_date_from_id(date_id)
    while dateInfo is None:
        date_id += 1
        dateInfo = get_date_from_id(date_id)
    return date_id, dateInfo


def estimate_id_with_date(date: date, lastest_date: date, lastest_id: int) -> int:
    # by calculate gap between lastest date and target date, we can get the id
    gap = (lastest_date - date).days
    return lastest_id - gap


def get_id_from_date(date: date) -> int | None:
    max_id = LASTEST_ID
    return search_id_with_date(date, max_id, "binary")


def _download_file(url: str, save_dir: str, timeout=3) -> str | None:
    print("Downloading: ", url)
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
        return None

    return downloaded_file


import os


def get_file_by_id(request_file: str, save_dir: str, date_id: int) -> bool:
    success = False
    url = id_to_url(date_id, request_file)
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
    try:
        filename = _download_file(url, save_dir)
        print("Downloaded: ", filename)
        success = False if filename is None else True

    except HTTPError as he:
        print("HTTPError: ", he)
        success = False
    except FileNotFoundError as fnfe:
        print("FileNotFoundError: ", fnfe)
        success = False
    except ContentTooShortError as ctse:
        print("ContentTooShortError: ", ctse)
        success = False
    except URLError as ue:
        print("URLError: ", ue)
        success = False
    except OSError as oe:
        print("Timeout Error: ", oe)

    if success:
        pass

    return success


def get_file_by_date(request_file: str, save_dir: str, date: date) -> bool:
    date_id = get_id_from_date(date=date)
    if date_id is not None:
        return get_file_by_id(request_file, save_dir, date_id)
    else:
        return False


def get_range_files(request_file: str, save_dir: str, start_date: date, end_date: date):
    date = start_date
    while date <= end_date:
        status = get_file_by_date(request_file, save_dir, date)
        print(date, status)
        date += timedelta(days=1)


def get_past_files(request_file: str, save_dir: str, days: int):
    date = LASTEST_DATE
    for _ in range(days):
        status = get_file_by_date(request_file, save_dir, date)
        print(date, status)
        date -= timedelta(days=1)


if __name__ == "__main__":
    with sync_playwright() as playwright:
        print("Getting lastest info...")
        LASTEST_ID, LASTEST_DATE = lastest_info(playwright)
        # status = get_file_by_date(
        #     request_file="WEBPXTICK_DT.zip",
        #     save_dir="downloads",
        #     date=date(2022, 8, 20),
        # )
        # print(status)
        get_range_files(
            request_file="WEBPXTICK_DT.zip",
            save_dir="downloads",
            start_date=date(2023, 8, 20),
            end_date=date(2023, 8, 24),
        )
        # get_past_files(request_file="WEBPXTICK_DT.zip", save_dir="downloads", days=10)
