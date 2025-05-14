import math
from typing import Any, Iterator, List, Optional

import requests


class Query:
    def __init__(
        self,
        domain: str,
        id: str,
        clauses: Optional[dict[str, Any]] = None,
        app_token: Optional[str] = None,
        page_size=100_000,
        verbose=True,
    ):
        """
        Build a query object for a Socrata dataset

        See <https://dev.socrata.com/docs/queries/> for definitions of the clauses.
        Supported clauses are:
            - select
            - where
            - group
            - having
            - limit
            - offset

        Note the clause "order" is not supported because it is used internally for pagination.

        Args:
            domain (str): base URL
            id (str): dataset ID
            clauses (dict, optional): query clauses
            app_token (str, optional): Socrata developer app token, or None
            page_size (int, optional): Page size. Defaults to 1 million.
            verbose (bool): If True (default), print progress

        Returns:
            Query
        """
        self.domain = domain
        self.id = id
        self.clauses = clauses or {}
        self.app_token = app_token
        self.page_size = page_size
        self.verbose = verbose

        self.url = self._build_url(domain=domain, id=id, clauses=clauses)
        self.n_rows = self._get_n_rows()
        self.n_pages = math.ceil(self.n_rows / page_size)

    def __iter__(self) -> Iterator[List[dict]]:
        """
        Download a dataset page by page

        Yields:
            Sequence of objects returned by download_records()
        """
        if self.verbose:
            print(
                f"Downloading dataset {self.domain} {self.id}: {self.n_rows} rows in {self.n_pages} page(s) of {self.page_size} rows each"
            )

        for i in range(self.n_pages):
            if self.verbose:
                print(f"  Downloading page {i + 1}/{self.n_pages}")

            start = i * self.page_size
            end = (i + 1) * self.page_size - 1
            page = self._get_records(start=start, end=end)

            assert len(page) > 0
            assert len(page) <= self.page_size

            yield page

    def _get_n_rows(self) -> int:
        """
        Get the number of rows in the query

        Returns:
            int: number of rows in the dataset
        """

        url = self._build_url(
            domain=self.domain,
            id=self.id,
            clauses=self.clauses | {"select": "count(:id)"},
        )
        result = self._get_request(url, app_token=self.app_token)

        assert len(result) == 1
        assert "count_id" in result[0]
        return int(result[0]["count_id"])

    def _get_records(self, start: int, end: int) -> List[dict]:
        """
        Download a specific range of rows from a dataset

        Args:
            start (int): first row (zero-indexed)
            end (int): last row (zero-indexed)

        Returns:
            List[dict]: list of records
        """

        assert end >= start
        limit = end - start + 1

        url = self._build_url(
            domain=self.domain,
            id=self.id,
            clauses=self.clauses | {"limit": limit, "offset": start},
        )

        return self._get_request(url, app_token=self.app_token)

    @staticmethod
    def _build_url(
        domain: str,
        id: str,
        clauses: Optional[dict[str, Any]] = None,
    ) -> str:
        """
        Build a URL for the Socrata API

        Args:
            domain (str): base URL
            id (str): dataset ID
            clauses (dict, optional): query clauses

        Returns:
            str: URL
        """
        url = f"https://{domain}/resource/{id}.json"

        if clauses is not None:
            assert set(clauses.keys()).issubset(
                ["select", "where", "group", "having", "limit", "offset"]
            )

            url += "?" + "&".join([f"${key}={value}" for key, value in clauses.items()])

        return url

    @staticmethod
    def _get_request(url: str, app_token: Optional[str]) -> List[dict]:
        payload = {}
        if app_token is not None:
            payload["X-App-token"] = app_token

        r = requests.get(url, data=payload)
        if r.status_code == 200:
            return r.json()
        else:
            raise RuntimeError(
                f"HTTP request failure: url '{url}' failed with code {r.status_code}"
            )
