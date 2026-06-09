import functools
import json
import urllib.error
import warnings
from collections.abc import Iterator, Sequence
from typing import Optional
from urllib.parse import urlencode, urlunparse
from urllib.request import Request, urlopen


def _page_bounds(
    offset: int, limit: int | None, page_size: int, n_where_records: int
) -> list[tuple[int, int]]:
    assert offset >= 0
    assert page_size >= 1
    assert limit is None or limit >= 0
    assert n_where_records >= 0

    # number of records to return in the absence of a limit
    n_nolimit = max(0, n_where_records - offset)

    if limit is None:
        n_to_return = n_nolimit
    else:
        n_to_return = min(limit, n_nolimit)

    bounds = []
    n_returned = 0
    while n_returned < n_to_return:
        this_limit = min(page_size, n_to_return - n_returned)
        bounds.append((offset + n_returned, this_limit))
        n_returned += this_limit

    return bounds


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
        bounds = _page_bounds(
            offset=self.offset,
            limit=self.limit,
            page_size=page_size,
            n_where_records=self._n_where_records,
        )
        n_pages = len(bounds)

        if self.verbose:
            print(
                f"Downloading {self.n_records} records in {n_pages} page(s) of "
                f"{page_size} records each..."
            )

        for i, (offset, limit) in enumerate(bounds):
            page = self._get_records(offset=offset, limit=limit)

            if self.verbose:
                print(f"  Downloaded page {i + 1}/{n_pages} with {len(page)} records")

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
                "No matching dataset records. This may be due to an bad query."
            )

        return n

    @property
    def n_records(self) -> int:
        """
        The number of records in the query. If the query has zero offset, no limit, and no
        WHERE clause, this is the number of records in the dataset.
        """
        if self._n_where_records == 0 or self.offset > self._n_where_records:
            return 0

        n_nolimit = self._n_where_records - self.offset

        if self.limit is None:
            return n_nolimit
        else:
            return min(self.limit, n_nolimit)

    def _get_records(self, offset: int, limit: int) -> list[dict]:
        """
        Download a specific range of records that satisfy the WHERE clause
        """

        assert offset >= 0
        assert limit >= 0

        return self._get_request(
            self.url,
            params=self._build_payload(
                select=self.select, where=self.where, offset=offset, limit=limit
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
        if params is not None:
            request_url = url + "?" + urlencode(params, doseq=True)
        else:
            request_url = url

        if app_token is not None:
            headers = {"X-App-token": app_token}
        else:
            headers = {}

        request = Request(request_url, headers=headers, method="GET")

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
