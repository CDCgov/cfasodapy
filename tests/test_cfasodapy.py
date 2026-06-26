import pytest

import cfasodapy


def test_build_url():
    assert (
        cfasodapy._build_url("data.cdc.gov", "path/to/query.json")
        == "https://data.cdc.gov/path/to/query.json"
    )


@pytest.mark.parametrize(
    ["select", "where", "expected"],
    [
        ("*", None, "SELECT *"),
        ("whatever I say", None, "SELECT whatever I say"),
        (["field1", "field2"], None, "SELECT `field1`,`field2`"),
        ("*", '`foo`="bar"', 'SELECT * WHERE `foo`="bar"'),
        ("field1", '`foo`="bar"', 'SELECT field1 WHERE `foo`="bar"'),
    ],
)
def test_build_query_string(select, where, expected):
    assert cfasodapy._build_query_string(select=select, where=where) == expected


class TestGet:
    n_records = 17
    page_size = 5

    @pytest.fixture
    @classmethod
    def mock_query(cls, monkeypatch):
        def mock_get_n_records(domain, id, app_token, where, verbose):
            return cls.n_records

        def mock_get_page(domain, id, app_token, query, page_number, page_size):
            start = page_size * (page_number - 1) + 1
            end = page_size * page_number

            if start > cls.n_records:
                return []
            else:
                return list(range(start, min(end, cls.n_records) + 1))

        monkeypatch.setattr(cfasodapy, "_get_n_records", mock_get_n_records)
        monkeypatch.setattr(cfasodapy, "_get_page", mock_get_page)

    def test_paging(self, mock_query):
        pages = list(
            cfasodapy.get_pages(
                domain="data.cdc.gov",
                id="abcd-1234",
                app_token="mytoken",
                page_size=self.page_size,
                verbose=False,
            )
        )
        assert len(pages) == 4
        assert [len(page) for page in pages] == [5, 5, 5, 2]
        assert [page[0] for page in pages] == [1, 6, 11, 16]
        assert [page[-1] for page in pages] == [5, 10, 15, 17]

    def test_get_all(self, mock_query):
        result = cfasodapy.get_all(
            domain="data.cdc.gov",
            id="abcd-1234",
            app_token="mytoken",
            page_size=self.page_size,
            verbose=False,
        )
        assert result == list(range(1, 17 + 1))
