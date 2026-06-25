import pytest

from cfasodapy import Query


@pytest.mark.parametrize(
    ["select", "where", "expected"],
    [
        (None, None, "SELECT *"),
        ("whatever I say", None, "SELECT whatever I say"),
        (["field1", "field2"], None, "SELECT `field1`,`field2`"),
        (None, '`foo`="bar"', 'SELECT * WHERE `foo`="bar"'),
        ("field1", '`foo`="bar"', 'SELECT field1 WHERE `foo`="bar"'),
    ],
)
def test_build_query_string(select, where, expected):
    assert Query._build_query_string(select=select, where=where) == expected


@pytest.fixture
def mock_query(monkeypatch):
    n_records = 17
    page_size = 5

    def mock_get_page(
        cls, url: str, app_token: str, query: str, page_number: int, page_size: int
    ):
        start = page_size * (page_number - 1) + 1
        end = page_size * page_number

        if start > n_records:
            return []
        else:
            return list(range(start, min(end, n_records) + 1))

    monkeypatch.setattr(Query, "n_records", n_records)
    monkeypatch.setattr(Query, "_get_page", mock_get_page)
    return Query(
        domain="data.cdc.gov",
        id="abcd-1234",
        app_token="mytoken",
        page_size=page_size,
        verbose=False,
    )


def test_build_url(mock_query):
    assert mock_query.url == "https://data.cdc.gov/api/v3/views/abcd-1234/query.json"


def test_paging(mock_query):
    pages = list(mock_query)
    assert len(pages) == 4
    assert [len(page) for page in pages] == [5, 5, 5, 2]
    assert [page[0] for page in pages] == [1, 6, 11, 16]
    assert [page[-1] for page in pages] == [5, 10, 15, 17]


def test_get_all(mock_query):
    result = mock_query.get_all()
    assert result == list(range(1, 17 + 1))
