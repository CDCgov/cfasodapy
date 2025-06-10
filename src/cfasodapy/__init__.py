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
            select (str or Sequence[str], optional): select clause (i.e., column
                name or comma-separated list of column names) or a list of columns
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
        Download all records from the dataset

        Returns:
            list[dict]: list of records
        """

        if self.verbose:
            print(f"Downloading dataset {self.domain} {self.id}: {self.n_rows} rows")

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
            print(f"  Downloaded {len(result)} rows")

        return result

    def get_pages(self, page_size: int = 10_000) -> Iterator[list[dict]]:
        """
        Download a dataset page by page

        Args:
            page_size (int): number of records per page (default: 10,000)

        Yields:
            Sequence of pages, each of which is a list of records
        """

        row_count = self.n_rows
        n_pages = _int_divide_ceiling(row_count, page_size)

        if self.verbose:
            print(
                f"Downloading dataset {self.domain} {self.id}: "
                f"{row_count} rows in {n_pages} page(s) of at most "
                f"{page_size} rows each..."
            )

        for i in range(n_pages):
            if self.verbose:
                print(f"  Downloading page {i + 1}/{n_pages}")

            start = i * page_size
            end = (i + 1) * page_size - 1
            page = self._get_records(start=start, end=end)

            assert len(page) > 0
            assert len(page) <= page_size

            yield page

    @property
    def n_rows(self) -> int:
        """
        The number of rows in the query

        Returns:
            int: number of rows in the dataset
        """
        result = self._get_request(
            self.url,
            params=self._build_payload(select="count(:id)", where=self.where, limit=1),
            app_token=self.app_token,
        )

        assert len(result) == 1, f"Expected length 1, got {len(result)}"
        assert "count_id" in result[0]
        n_dataset_rows = int(result[0]["count_id"])

        if n_dataset_rows == 0:
            if self.verbose:
                warnings.warn(
                    f"Dataset {self.id} at {self.domain} has no rows. "
                    "This may be due to an bad query."
                )
            return 0

        n_rows_after_offset = n_dataset_rows - self.offset

        if n_rows_after_offset < 0:
            if self.verbose:
                warnings.warn(
                    f"Offset {self.offset} is larger than the number of rows"
                    f" in the dataset ({n_dataset_rows})."
                )
            return 0

        if self.limit is None or self.limit > n_rows_after_offset:
            return n_rows_after_offset
        else:
            return n_rows_after_offset - self.limit

    def _get_records(self, start: int, end: int) -> list[dict]:
        """
        Download a specific range of rows from a query, accounting
        for the offset and limit.

        Args:
            start (int): first row (zero-indexed)
            end (int): last row (zero-indexed)

        Returns:
            list[dict]: list of records
        """

        assert end >= start
        assert (
            self.limit is None or end < self.limit
        ), f"End index {end} is larger than limit {self.limit}."
        n_rows = end - start + 1

        return self._get_request(
            self.url,
            params=self._build_payload(
                select=self.select,
                where=self.where,
                offset=self.offset + start,
                limit=n_rows,
            ),
            app_token=self.app_token,
        )

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
            for x in select:
                assert "," not in x, f"Comma(s) detected in select column name: {x}"
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
