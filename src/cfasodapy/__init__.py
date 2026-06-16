import functools
import json
import urllib.error
import warnings
from collections.abc import Sequence
from typing import Optional
from urllib.parse import urlunparse
from urllib.request import Request, urlopen

from typing_extensions import Self


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

        Returns:
            Query
        """
        self.domain = domain
        self.id = id
        self.app_token = app_token
        self.select = select
        self.where = where
        self.page_size = page_size
        self.verbose = verbose

        self.page_number = 1
        self.url = urlunparse(
            ("https", self.domain, f"api/v3/views/{self.id}/query.json", "", "", "")
        )
        self.n_pages = _int_divide_ceiling(self.n_records, self.page_size)

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

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> list[dict]:
        result = self._get_request(
            self.url,
            app_token=self.app_token,
            query=self._build_query_string(select=self.select, where=self.where),
            page_number=self.page_number,
            page_size=self.page_size,
        )

        if len(result) == 0:
            raise StopIteration

        if self.verbose:
            print(
                f"  Downloaded page {self.page_number}/{self.n_pages} with {len(result)} records"
            )

        self.page_number += 1
        return result

    @functools.cached_property
    def n_records(self) -> int:
        """Number of records in the dataset that satisfy the WHERE clause"""
        result = self._get_request(
            self.url,
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

    @classmethod
    def _get_request(
        cls, url: str, app_token: str, query: str, page_number: int, page_size: int
    ) -> list[dict]:
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
