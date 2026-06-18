import functools
import itertools
import json
import urllib.error
import warnings
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlunparse
from urllib.request import Request, urlopen


@dataclass
class Query:
    """
    Query a Socrata dataset

    Note that GROUP BY, HAVING, ORDER BY, LIMIT, and OFFSET clauses are not supported.

    Args:
        domain: base URL
        id: dataset ID
        app_token: Socrata developer app token
        select: SELECT clause. A single string (like a column name) will be used as-is.
            A list of strings will be backtick-quoted and comma-joined.
        where: WHERE clause. This is the filter condition.
        page_size: page size
        verbose: Print progress messages and warnings.
    """

    domain: str
    id: str
    app_token: str
    select: str | Sequence[str] | None = None
    where: str | None = None
    page_size: int = 10_000
    verbose: bool = True

    def __post_init__(self):
        if self.n_records == 0 and self.verbose:
            warnings.warn(
                "No matching dataset records. This may be due to an bad query."
            )

    def get_all(self) -> list[dict]:
        """
        Download all records from the query. This is a convenience function for `[x for page in query for x in page]`.

        Returns:
            list[dict]: list of records
        """

        if self.verbose:
            print(f"Downloading {self.n_records} records")

        result = [x for page in self for x in page]

        if self.verbose:
            print(f"  Downloaded {len(result)} records")

        return result

    def __iter__(self) -> Iterator[list[dict]]:
        """
        Iterate over pages in the query, for example `for page in my_query`
        """
        n_pages = _int_divide_ceiling(self.n_records, self.page_size)

        for page_number in itertools.count(start=1):
            result = _get_page(
                url=self._url,
                app_token=self.app_token,
                query=_build_query_string(select=self.select, where=self.where),
                page_number=page_number,
                page_size=self.page_size,
            )

            if len(result) == 0:
                break

            if self.verbose:
                print(
                    f"  Downloaded page {page_number}/{n_pages} with {len(result)} records"
                )

            yield result

    def get_column_types(self) -> list[tuple[str, str]]:
        """
        Returns:
            list of (field name, data type) pairs
        """
        url = f"https://{self.domain}/api/views/{self.id}.json"
        r = _get_request(url=url, app_token=self.app_token, method="GET")
        return [(x["fieldName"], x["dataTypeName"]) for x in r["columns"]]

    @property
    def _url(self) -> str:
        """Query API endpoint URL"""
        return urlunparse(
            ("https", self.domain, f"api/v3/views/{self.id}/query.json", "", "", "")
        )

    @property
    def n_records(self) -> int:
        """Number of records in the dataset that satisfy the WHERE clause"""
        return _get_n_records(url=self._url, app_token=self.app_token, where=self.where)


@functools.cache
def _get_n_records(url, app_token, where):
    result = _get_page(
        url=url,
        app_token=app_token,
        query=_build_query_string(select="count(:id)", where=where),
        page_number=1,
        page_size=10,
    )

    assert len(result) == 1, f"Expected length 1, got {len(result)}"
    assert "count_id" in result[0]
    return int(result[0]["count_id"])


def _build_query_string(
    select: Optional[str | Sequence[str]] = None, where: Optional[str] = None
) -> str:
    """
    Build the query string for the request
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
