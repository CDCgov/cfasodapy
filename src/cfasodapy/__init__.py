import itertools
import json
import urllib.error
import warnings
from collections.abc import Iterator, Sequence
from typing import Any
from urllib.parse import urlunparse
from urllib.request import Request, urlopen


def get_pages(
    domain: str,
    id: str,
    app_token: str,
    select: str | Sequence[str] = "*",
    where: str | None = None,
    page_size: int = 10_000,
    verbose=True,
) -> Iterator[list[dict]]:
    """
    Query a Socrata dataset, page by page

    Note that GROUP BY, HAVING, ORDER BY, LIMIT, and OFFSET clauses are not supported.

    Args:
        domain: base URL
        id: dataset ID
        app_token: Socrata developer app token
        select: SELECT clause. A list of strings that will be backtick-quoted and comma-joined.
        where: filter condition
        page_size: page size
        verbose (bool): If True (default), print progress and warnings.

    Returns: iterator over pages
    """
    if page_size <= 0:
        raise ValueError("page_size must be a positive integer")

    n_records = _get_n_records(
        domain=domain, id=id, app_token=app_token, where=where, verbose=verbose
    )
    n_pages = _int_divide_ceiling(n_records, page_size)
    for page_number in itertools.count(start=1):
        result = _get_page(
            domain=domain,
            id=id,
            app_token=app_token,
            query=_build_query_string(select=select, where=where),
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
    select: str | Sequence[str] = "*",
    where: str | None = None,
    page_size: int = 10_000,
    verbose=True,
) -> list[dict]:
    """
    Download all records from the query. This is a convenience function for
    `[x for page in get_pages(...) for x in page]`.

    Returns: list of records
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
    return [x for page in pages for x in page]


def _get_n_records(
    domain: str, id: str, app_token: str, where: str | None = None, verbose: bool = True
) -> int:
    """
    Number of records in the dataset that satisfy the WHERE clause.

    This value is cached and may not reflect updates to the Query's domain, ID,
    or WHERE clause.
    """
    result = _get_page(
        domain=domain,
        id=id,
        app_token=app_token,
        query=_build_query_string(select="count(:id)", where=where),
        page_number=1,
        page_size=10,
    )

    if len(result) != 1:
        raise RuntimeError(f"Expected 1 count result, got {len(result)}")
    try:
        n = int(result[0]["count_id"])
    except (KeyError, TypeError, ValueError) as e:
        raise RuntimeError("Malformed count response: expected key 'count_id' with an integer value") from e

    if n == 0 and verbose:
        warnings.warn("No matching dataset records. This may be due to a bad query.")
    return n


def get_column_types(domain: str, id: str, app_token: str) -> list[tuple[str, str]]:
    """
    Column names and types. Note that the column types are reported as they are
    annotated in the dataset. They are not parsed or validated programmatically
    by `cfasodapy`, and they may not be accurate.

    Returns:
        list of (field name, data type) pairs
    """

    url = _build_url(domain, f"api/views/{id}.json")
    r = _get_request(url=url, app_token=app_token, method="GET")
    return [(x["fieldName"], x["dataTypeName"]) for x in r["columns"]]


def _build_query_string(select: str | Sequence[str], where: str | None) -> str:
    """
    Build the query string for the request
    """
    s = "SELECT "
    if isinstance(select, str):
        s += select
    else:
        s += ",".join([f"`{x}`" for x in select])

    if where is not None:
        s += " WHERE " + where

    return s


def _build_url(domain: str, path: str) -> str:
    return urlunparse(("https", domain, path, "", "", ""))


def _get_page(
    domain: str,
    id: str,
    app_token: str,
    query: str,
    page_number: int,
    page_size: int,
) -> list[dict]:
    url = _build_url(domain, f"api/v3/views/{id}/query.json")

    # query, etc. are called "request options" <https://dev.socrata.com/docs/queries/>
    options = {
        "query": query,
        "page": {"pageNumber": page_number, "pageSize": page_size},
        "includeSynthetic": False,
    }

    return _get_request(url=url, app_token=app_token, payload=options, method="POST")


def _get_request(
    url: str, app_token: str, method: str, payload: dict | None = None
) -> Any:
    if payload is None:
        data = None
    else:
        data = json.dumps(payload).encode("utf-8")

    headers = {"X-App-token": app_token, "Content-Type": "application/json"}
    request = Request(url, data=data, headers=headers, method=method)

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
