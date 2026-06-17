import pytest

import cfasodapy


def test_build_url():
    assert (
        cfasodapy._build_url(domain="data.cdc.gov", id="abcd-1234")
        == "https://data.cdc.gov/api/v3/views/abcd-1234/query.json"
    )


@pytest.mark.parametrize(
    ["select", "where", "expected"],
    [
        # no explicit select produces *
        (None, None, "SELECT *"),
        # if it's a string, just plop it in wholesale
        # list of strings get backtick-quoted & comma-joined
        (["field1", "field2"], None, "SELECT `field1`,`field2`"),
        # where clause but not select clause
        (None, '`foo`="bar"', 'SELECT * WHERE `foo`="bar"'),
        # both select and where
        ("field1", '`foo`="bar"', 'SELECT field1 WHERE `foo`="bar"'),
    ],
)
def test_build_query(select, where, expected):
    assert cfasodapy._build_query(select=select, where=where) == expected


class TestGet:
    n_records = 17
    page_size = 5

    kwargs = {
        "domain": "data.cdc.gov",
        "id": "abcd-1234",
        "app_token": "mytoken",
        "page_size": page_size,
        "verbose": False,
    }

    @pytest.fixture
    def mock_request(self, monkeypatch):
        def mock_get_n_records(*args, **kwargs):
            return self.n_records

        def mock_get_page(url, app_token, query, page_number, page_size):
            start = page_size * (page_number - 1) + 1
            end = page_size * page_number

            if start > self.n_records:
                return []
            else:
                return list(range(start, min(end, self.n_records) + 1))

        monkeypatch.setattr(cfasodapy, "_get_n_records", mock_get_n_records)
        monkeypatch.setattr(cfasodapy, "_get_page", mock_get_page)

    def test_get_pages(self, mock_request):
        pages_iter = cfasodapy.get_pages(**self.kwargs)
        pages = list(pages_iter)

        assert len(pages) == 4
        assert [len(page) for page in pages] == [5, 5, 5, 2]
        assert [page[0] for page in pages] == [1, 6, 11, 16]
        assert [page[-1] for page in pages] == [5, 10, 15, 17]

    def test_get_all(self, mock_request):
        result = cfasodapy.get_all(**self.kwargs)
        assert result == list(range(1, 17 + 1))
