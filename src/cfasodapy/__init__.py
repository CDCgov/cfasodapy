import functools
import itertools
import warnings
from collections.abc import Iterator, Sequence
from typing import Optional
from urllib.parse import urlunparse

import requests


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

        starts = [self._start_record]
        ends = []
        x = self._start_record

        starts = list(range(self._start_record, self._end_record, page_size))
        n_pages = len(starts) - 1

        if self.verbose:
            print(
                f"Downloading {self.n_records} records in {n_pages} page(s) of "
                f"{page_size} records each..."
            )

        for i, (start, next_start) in enumerate(itertools.pairwise(starts)):
            if self.verbose:
                print(f"  Downloading page {i + 1}/{n_pages}")

            page = self._get_records(start=start, end=next_start - 1)

            assert len(page) == page_size

            yield page

        # partial, final page
        if next_start - 1 < self._end_record:
            page = self._get_records(start=next_start, end=self._end_record)
            assert 0 < len(page) < page_size
            yield page

    @functools.cached_property
    def _n_where_records(self) -> int:
        """Number of records in the dataset that satisfy the WHERE clause"""
        result = self._get_request(
            self.url,
            params=self._build_payload(select="count(:id)", where=self.where, limit=1),
            app_token=self.app_token,
        )

        assert len(result) == 1, f"Expected length 1, got {len(result)}"
        assert "count_id" in result[0]
        n = int(result[0]["count_id"])

        if n == 0 and self.verbose:
            warnings.warn(
                f"Dataset {self.id} at {self.domain} has no records. "
                "This may be due to an bad query."
            )

        return n

    @property
    def _start_record(self) -> int:
        return min(self.offset, self._n_where_records - 1)

    @property
    def _end_record(self) -> int:
        if self._n_where_records == 0:
            return 0
        elif self.limit is None:
            return self._n_where_records - 1
        else:
            return min(self._n_where_records - 1, self._start_record + self.limit - 1)

    @property
    def n_records(self) -> int:
        """
        The number of records in the query. If the query has zero offset, no limit, and no
        WHERE clause, this is the number of records in the dataset.
        """
        return self._end_record - self._start_record

    def _get_records(self, start: int, end: int) -> list[dict]:
        """
        Download a specific range of records that satisfy the WHERE clause

        Args:
            start: first record, zero-indexed
            end: last record, zero-indexed

        Returns:
            records
        """

        assert start <= end
        limit = end - start + 1

        return self._get_request(
            self.url,
            params=self._build_payload(
                select=self.select, where=self.where, offset=start, limit=limit
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
