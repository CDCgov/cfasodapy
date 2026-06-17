import itertools
import json
import urllib.error
import warnings
from collections.abc import Iterator, Sequence
from typing import Optional
from urllib.parse import urlunparse
from urllib.request import Request, urlopen


def get_pages(
    domain: str,
    id: str,
    app_token: str,
    select: Optional[str | Sequence[str]] = None,
    where: Optional[str] = None,
    page_size: int = 10_000,
    verbose=True,
) -> Iterator[list[dict]]:
    """
    Iterate over pages in a Socrata dataset

    Note that GROUP BY, HAVING, ORDER BY, LIMIT, and OFFSET clauses are not supported.

    Args:
        domain (str): base URL
        id (str): dataset ID
        app_token (str): Socrata developer app token
        select (str or Sequence[str], optional): select clause
            (e.g., a column name or a comma-separated list of column names)
            or a list of strings that will be backtick-quoted and comma-joined
            (e.g., a list of column names)
        where (str, optional): filter condition
        page_size (int, optional): page size
        verbose (bool): If True (default), print progress and warnings.

    Returns:
        iterator over pages
    """
    url = _build_url(domain=domain, id=id)
    query = _build_query(select=select, where=where)
    n_records = _get_n_records(
        url=url, app_token=app_token, where=where, verbose=verbose
    )
    n_pages = _int_divide_ceiling(n_records, page_size)

    if verbose:
        print(f"Downloading {n_records} records")

    for page_number in itertools.count(start=1):
        result = _get_page(
            url=url,
            app_token=app_token,
            query=query,
            page_number=page_number,
            page_size=page_size,
        )

        if len(result) == 0:
            break

        if verbose:
            print(
                f"  Downloaded page {page_number}/{n_pages} with {len(result)} records"
            )

        yield result


def get_all(
    domain: str,
    id: str,
    app_token: str,
    select: Optional[str | Sequence[str]] = None,
    where: Optional[str] = None,
    page_size: int = 10_000,
    verbose=True,
) -> list[dict]:
    """
    Download all records from the query. This is a convenience function for
    `[x for page in get_pages(...) for x in page]`.

    Returns:
        list[dict]: list of records
    """

    pages = get_pages(
        domain=domain,
        id=id,
        app_token=app_token,
        select=select,
        where=where,
        page_size=page_size,
        verbose=verbose,
    )
    result = [x for page in pages for x in page]
    return result


def _build_url(domain: str, id: str) -> str:
    """Get the API endpoint URL"""
    return urlunparse(("https", domain, f"api/v3/views/{id}/query.json", "", "", ""))


def _get_n_records(url: str, app_token: str, where: str | None, verbose: bool) -> int:
    """Get the number of records in the dataset that satisfy the WHERE clause"""
    result = _get_page(
        url=url,
        app_token=app_token,
        query=_build_query(select="count(:id)", where=where),
        page_number=1,
        page_size=10,
    )

    assert len(result) == 1, f"Expected length 1, got {len(result)}"
    assert "count_id" in result[0]
    n = int(result[0]["count_id"])

    if n == 0 and verbose:
        warnings.warn("No matching dataset records. This may be due to a bad query.")
    return n


def _build_query(
    select: Optional[str | Sequence[str]] = None, where: Optional[str] = None
) -> str:
    """
    Build the SoQL query string
    """
    s = "SELECT "
    if select is None:
        s += "*"
    elif isinstance(select, str):
        s += select
    else:
        s += ",".join([f"`{x}`" for x in select])

    if where is not None:
        s += " WHERE " + where

    return s


def _get_page(
    url: str, app_token: str, query: str, page_number: int, page_size: int
) -> list[dict]:
    """Get data from a single page"""
    # query, etc. are called "request options" <https://dev.socrata.com/docs/queries/>
    options = {
        "query": query,
        "page": {"pageNumber": page_number, "pageSize": page_size},
        "includeSynthetic": False,
    }

    data = json.dumps(options).encode("utf-8")
    headers = {"X-App-token": app_token, "Content-Type": "application/json"}
    request = Request(url, data=data, headers=headers, method="POST")

    try:
        with urlopen(request) as response:
            return json.load(response)
    except urllib.error.HTTPError as e:
        msg = "\n".join(
            [
                f"HTTP Error {e.code} {e.reason}",
                f"URL: {e.url}",
                e.read().decode("utf-8", errors="replace"),
            ]
        )

        raise RuntimeError(msg) from e


def _int_divide_ceiling(a: int, b: int) -> int:
    """
    Equivalent of a // b, but with
    ceiling rather than floor behavior.

    Follows the implementation here: https://stackoverflow.com/a/17511341

    Args:
        a (int): dividend
        b (int): divisor

    Returns:
        int: (1 + a // b) if a is not divisible by b, otherwise (a // b)
    """
    return -(a // -b)
