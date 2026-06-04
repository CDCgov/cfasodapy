import functools
import warnings
from collections.abc import Iterator, Sequence
from typing import Optional
from urllib.parse import urlunparse

import requests


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


class Query:
    def __init__(
        self,
        domain: str,
        id: str,
        select: Optional[str | Sequence[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        app_token: Optional[str] = None,
        verbose=True,
    ):
        """
        Build a query for a Socrata dataset

        Note that $group, $having, and $order clauses are not supported.

        Args:
            domain (str): base URL
            id (str): dataset ID
            select (str or Sequence[str], optional): select clause
                (e.g., a column name or a comma-separated list of column names)
                or a list of strings that will be comma-joined (e.g., a list
                of column names)
            where (str, optional): filter condition
            limit (int, optional): maximum number of records to return
            offset (int): number of records to skip. Default: 0.
            app_token (str, optional): Socrata developer app token, or None
            verbose (bool): If True (default), print progress and warnings.

        Returns:
            Query
        """
        self.domain = domain
        self.id = id
        self.select = select
        self.where = where
        self.limit = limit
        self.offset = offset
        self.app_token = app_token
        self.verbose = verbose

    def get_all(self) -> list[dict]:
        """
        Download all records from the query

        Returns:
            list[dict]: list of records
        """

        if self.verbose:
            print(f"Downloading {self.n_records} records")

        result = self._get_request(
            self.url,
            params=self._build_payload(
                select=self.select,
                where=self.where,
                limit=self.limit,
                offset=self.offset,
            ),
            app_token=self.app_token,
        )

        if self.verbose:
            print(f"  Downloaded {len(result)} records")

        return result

    def get_pages(self, page_size: int = 10_000) -> Iterator[list[dict]]:
        """
        Download a dataset page by page

        Args:
            page_size: number of records per page

        Yields:
            Sequence of pages, each of which is a list of records
        """

        n_pages = _int_divide_ceiling(self.n_records, page_size)

        if self.verbose:
            print(
                f"Downloading {self.n_records} records in {n_pages} page(s) of "
                f"{page_size} records each..."
            )

        offset = self.offset
        i = 1
        page = None

        while page is None or len(page) == page_size:
            page = self._get_page(offset=offset, limit=page_size)

            if len(page) > 0:
                if self.verbose:
                    print(f"  Downloaded page {i}/{n_pages} with {len(page)} records")

                yield page
                offset += page_size
                i += 1

    def _get_page(self, offset: int, limit: int) -> list[dict]:
        return self._get_request(
            self.url,
            params=self._build_payload(
                select=self.select, where=self.where, offset=offset, limit=limit
            ),
            app_token=self.app_token,
        )

    def _get_n_where(self) -> int:
        """Number of records that satisfy the WHERE clause"""
        result = self._get_request(
            self.url,
            params=self._build_payload(select="count(:id)", where=self.where, limit=1),
            app_token=self.app_token,
        )

        assert len(result) == 1, f"Expected length 1, got {len(result)}"
        assert "count_id" in result[0]
        return int(result[0]["count_id"])

    @functools.cached_property
    def n_records(self) -> int:
        """Inferred number of records in the query"""
        n_where = self._get_n_where()

        if n_where == 0:
            if self.verbose:
                warnings.warn(
                    "No records satisfy the WHERE clause. This may be due to an bad query."
                )
            return 0
        elif self.offset >= n_where:
            if self.verbose:
                warnings.warn(
                    "Offset is greater than number of records that satisfy WHERE clause."
                )
            return 0
        elif self.limit is None:
            return n_where - self.offset
        else:
            return min(n_where - self.offset, self.limit)

    @classmethod
    def _build_payload(
        cls,
        select: Optional[str | Sequence[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> dict:
        """
        Build the payload for the request
        """
        clauses = {}

        if select is None:
            pass
        elif isinstance(select, str):
            clauses["$select"] = select
        else:
            clauses["$select"] = ",".join(select)

        if where is not None:
            clauses["$where"] = where

        if limit is not None:
            assert isinstance(limit, int)
            assert limit > 0
            clauses["$limit"] = limit

        assert isinstance(offset, int)
        assert offset >= 0
        clauses["$offset"] = offset

        return clauses

    @property
    def url(self) -> str:
        """
        Socrata API base URL for the query

        Returns:
            str: URL
        """
        return urlunparse(
            ("https", self.domain, f"resource/{self.id}.json", "", "", "")
        )

    @classmethod
    def _get_request(
        cls, url: str, params: Optional[dict] = None, app_token: Optional[str] = None
    ) -> list[dict]:
        if app_token is not None:
            data = {"X-App-token": app_token}
        else:
            data = None

        r = requests.get(url, data=data, params=params)
        r.raise_for_status()
        return r.json()
