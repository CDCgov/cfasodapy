from typing import Any, Iterator, List, Optional
from urllib.parse import urlunparse

import requests


def _int_divide_ceiling(a, b):
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
        clauses: Optional[dict[str, Any]] = None,
        app_token: Optional[str] = None,
        verbose=True,
    ):
        """
        Build a query for a Socrata dataset

        See <https://dev.socrata.com/docs/queries/> for definitions of the clauses.
        Supported clauses are:
            - $select
            - $where
            - $group
            - $having
            - $limit
            - $offset
            - $order

        Args:
            domain (str): base URL
            id (str): dataset ID
            clauses (dict, optional): query clauses
            app_token (str, optional): Socrata developer app token, or None
            verbose (bool): If True (default), print progress

        Returns:
            Query
        """
        self.domain = domain
        self.id = id
        self.clauses = clauses or {}
        self.app_token = app_token
        self.verbose = verbose

    def get_all(self) -> List[dict]:
        """
        Download all records from the dataset

        Returns:
            List[dict]: list of records
        """

        if self.verbose:
            print(f"Downloading dataset {self.domain} {self.id}: {self.n_rows} rows")

        result = self._get_request(
            self.url,
            params=self.clauses,
            app_token=self.app_token,
        )

        if self.verbose:
            print(f"  Downloaded {len(result)} rows")

        return result

    def get_pages(self, page_size: int = 10_000) -> Iterator[List[dict]]:
        """
        Download a dataset page by page

        Args:
            page_size (int): number of records per page (default: 10,000)

        Queries involving "$limit", "$offset", and "$order" are not supported
        because they are used internally for pagination.

        Yields:
            Sequence of pages, each of which is a list of records
        """
        if bad_keys := set(self.clauses.keys()).intersection(
            {"$limit", "$offset", "$order"}
        ):
            raise RuntimeError(
                f"Clause keys {bad_keys} are not supported in paginated queries."
            )

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
            params=self.clauses | {"$select": "count(:id)", "$limit": 1},
            app_token=self.app_token,
        )

        assert len(result) == 1, f"Expected length 1, got {len(result)}"
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

        return self._get_request(
            self.url,
            params=self.clauses | {"$limit": limit, "$offset": start},
            app_token=self.app_token,
        )

    @staticmethod
    def _validate_clauses(clauses: Optional[dict] = None) -> None:
        """
        Validate the query clauses

        Args:
            clauses (dict, optional): query clauses

        Raises:
            RuntimeError: if the clauses are invalid
        """
        keys = ["$select", "$where", "$group", "$having", "$limit", "$offset"]

        if clauses is None:
            pass
        else:
            assert isinstance(clauses, dict), "Clauses must be a dictionary."
            if bad_keys := set(clauses.keys()) - set(keys):
                raise RuntimeError(
                    f"Invalid clause keys: {bad_keys}. Supported keys are: {keys}."
                )

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
    ) -> List[dict]:
        cls._validate_clauses(params)

        if app_token is not None:
            data = {"X-App-token": app_token}
        else:
            data = None

        r = requests.get(url, data=data, params=params)
        if r.status_code == 200:
            return r.json()
        else:
            raise RuntimeError(
                f"HTTP request failure: url '{url}' failed with code {r.status_code}"
            )
