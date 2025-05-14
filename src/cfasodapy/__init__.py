import math
from typing import Iterable, List, Optional

import requests


def _n_dataset_rows(domain: str, id: str, app_token: Optional[str] = None) -> int:
    """
    Get the number of rows in a dataset

    Args:
        domain (str): base URL
        id (str): dataset ID
        app_token (str, optional): Socrata developer app token, or None

    Returns:
        int: number of rows in the dataset
    """
    url = f"https://{domain}/resource/{id}.json?$select=count(:id)"
    r = _get_request(url, app_token=app_token)

    result = r.json()
    assert len(result) == 1
    assert "count_id" in result[0]
    return int(result[0]["count_id"])


def download_dataset_records(
    domain: str,
    id: str,
    start_record: int,
    end_record: int,
    app_token: Optional[str],
) -> list[dict]:
    """
    Download a specific range of rows from a dataset

    Args:
        domain (str, optional): base URL
        id (str): dataset ID
        start_record (int): first row (zero-indexed)
        end_record (int): last row (zero-indexed)
        app_token (str, optional): Socrata developer app token, or None

    Returns:
        If format is "json", a list. If "csv", then a string
    """

    assert end_record >= start_record
    limit = end_record - start_record + 1

    url = f"https://{domain}/resource/{id}.json?$limit={limit}&$offset={start_record}&$order=:id"
    r = _get_request(url, app_token=app_token)

    return r.json()


def _get_request(url: str, app_token: Optional[str]) -> requests.Response:
    payload = {}
    if app_token is not None:
        payload["X-App-token"] = app_token

    r = requests.get(url, data=payload)
    if r.status_code == 200:
        return r
    else:
        raise RuntimeError(
            f"HTTP request failure: url '{url}' failed with code {r.status_code}"
        )


def download_dataset_pages(
    domain: str,
    id: str,
    app_token: Optional[str],
    page_size: int = int(1e5),
    verbose: bool = True,
) -> Iterable[List[dict]]:
    """
    Download a dataset page by page

    Args:
        domain (str): base URL
        id (str): dataset ID
        app_token (str, optional): Socrata developer app token, or None
        page_size (int, optional): Page size. Defaults to 1 million.
        verbose (bool): If True (default), print progress

    Yields:
        Sequence of objects returned by download_dataset_records()
    """
    n_rows = _n_dataset_rows(domain=domain, id=id, app_token=app_token)
    n_pages = math.ceil(n_rows / page_size)

    if verbose:
        print(
            f"Downloading dataset {id=}: {n_rows} rows in {n_pages} page(s) of {page_size} rows each"
        )

    for i in range(n_pages):
        if verbose:
            print(f"  Downloading page {i + 1}/{n_pages}")

        start_record = i * page_size
        end_record = (i + 1) * page_size - 1
        page = download_dataset_records(
            domain=domain,
            id=id,
            start_record=start_record,
            end_record=end_record,
            app_token=app_token,
        )

        assert len(page) > 0
        assert len(page) <= page_size

        yield page
