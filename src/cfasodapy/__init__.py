import functools
import itertools
import json
import urllib.error
import warnings
from collections.abc import Iterator, Sequence
from typing import Any, Optional
from urllib.parse import urlunparse
from urllib.request import Request, urlopen


class Query:
    def __init__(
        self,
        domain: str,
        id: str,
        app_token: str,
        select: Optional[str | Sequence[str]] = None,
        where: Optional[str] = None,
        page_size: int = 10_000,
        verbose=True,
    ):
        """
        Query a Socrata dataset

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
        """
        self.domain = domain
        self.id = id
        self.app_token = app_token
        self.select = select
        self.where = where
        self.page_size = page_size
        self.verbose = verbose

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
        n_pages = _int_divide_ceiling(self.n_records, self.page_size)

        for page_number in itertools.count(start=1):
            result = self._get_page(
                domain=self.domain,
                id=self.id,
                app_token=self.app_token,
                query=self._build_query_string(select=self.select, where=self.where),
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

    @functools.cached_property
    def n_records(self) -> int:
        """
        Number of records in the dataset that satisfy the WHERE clause.

        This value is cached and may not reflect updates to the Query's domain, ID,
        or WHERE clause.
        """
        result = self._get_page(
            domain=self.domain,
            id=self.id,
            app_token=self.app_token,
            query=self._build_query_string(select="count(:id)", where=self.where),
            page_number=1,
            page_size=10,
        )

        assert len(result) == 1, f"Expected length 1, got {len(result)}"
        assert "count_id" in result[0]
        n = int(result[0]["count_id"])

        if n == 0 and self.verbose:
            warnings.warn(
                "No matching dataset records. This may be due to an bad query."
            )

        return n

    @functools.cached_property
    def column_types(self) -> list[tuple[str, str]]:
        """
        Column names and types. Note that the column types are reported as they are
        annotated in the dataset. They are not parsed or validated programmatically
        by `cfasodapy`, and they may not be accurate.

        This value is cached and will not reflect updates to the Query's domain or ID.

        Returns:
            list of (field name, data type) pairs
        """

        url = self._build_url(self.domain, f"api/views/{self.id}.json")
        r = self._get_request(url=url, app_token=self.app_token, method="GET")
        return [(x["fieldName"], x["dataTypeName"]) for x in r["columns"]]

    @staticmethod
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

    @staticmethod
    def _build_url(domain: str, path: str) -> str:
        return urlunparse(("https", domain, path, "", "", ""))

    @classmethod
    def _get_page(
        cls,
        domain: str,
        id: str,
        app_token: str,
        query: str,
        page_number: int,
        page_size: int,
    ) -> list[dict]:
        url = cls._build_url(domain, f"api/v3/views/{id}/query.json")

        # query, etc. are called "request options" <https://dev.socrata.com/docs/queries/>
        options = {
            "query": query,
            "page": {"pageNumber": page_number, "pageSize": page_size},
            "includeSynthetic": False,
        }

        return cls._get_request(
            url=url, app_token=app_token, payload=options, method="POST"
        )

    @staticmethod
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
