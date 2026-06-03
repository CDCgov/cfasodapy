import pytest

from cfasodapy import Query, _page_bounds


def test_build_url():
    """
    Test the _build_url method of the Query class.
    """
    domain = "data.cdc.gov"
    id = "abc123"
    expected_url = f"https://{domain}/resource/{id}.json"

    assert Query(domain=domain, id=id).url == expected_url


def test_build_payload_select_string():
    select = "field1"
    expected_payload = {"$select": "field1", "$offset": 0}

    assert Query._build_payload(select=select) == expected_payload


def test_build_payload_select_list():
    select = ["field1", "field2"]
    expected_payload = {"$select": "field1,field2", "$offset": 0}

    assert Query._build_payload(select=select) == expected_payload


@pytest.mark.parametrize(
    ["n_where_records", "offset", "limit", "expected"],
    [
        # page size is 5 in all test
        # 10 records, 0 offset, no limit => 2 pages
        (10, 0, None, [(0, 5), (5, 5)]),
        # 1 offset is still 2 pages, but second page is shorter
        (10, 1, None, [(1, 5), (6, 4)]),
        # 1 offset, but not hitting the limit, means two pages
        (100, 1, 10, [(1, 5), (6, 5)]),
        # offset of 1 means 2 pages, one shorter
        (10, 1, None, [(1, 5), (6, 4)]),
        # no offset, limit of 5 means one page
        (10, 0, 5, [(0, 5)]),
        # or, can have an even shorter page
        (10, 0, 3, [(0, 3)]),
        # offset beyond length of dataset gives no pages
        (10, 15, None, []),
    ],
)
def test_page_bounds(n_where_records, offset, limit, expected):
    page_size = 5
    assert (
        _page_bounds(
            offset=offset,
            limit=limit,
            page_size=page_size,
            n_where_records=n_where_records,
        )
        == expected
    )


@pytest.mark.parametrize(
    ["offset", "limit", "expected"],
    [
        # zero offset, no limit => all 10 records
        (0, None, 10),
        # 5 offset => 5 records
        (5, None, 5),
        # limit of 5 => 5 records
        (0, 5, 5),
        # big offset => no records
        (20, None, 0),
        # limit and offset, no interaction
        (5, 3, 3),
        # limit and offset with interaction
        (8, 10, 2),
    ],
)
def test_n_records(monkeypatch, offset, limit, expected):
    n_where = 10
    monkeypatch.setattr(Query, "_n_where_records", n_where)

    q = Query(domain="data.cdc.gov", id="abc123", offset=offset, limit=limit)
    assert q.n_records == expected


def test_pages(monkeypatch):
    n_where = 10

    def mock_get_records(_, offset, limit):
        return list(range(offset, offset + limit))

    monkeypatch.setattr(Query, "_get_records", mock_get_records)
    monkeypatch.setattr(Query, "_n_where_records", n_where)

    q = Query(domain="data.cdc.gov", id="abc123", offset=0, limit=None)
    pages = list(q.get_pages(page_size=5))
    assert len(pages) == 2
    assert pages == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
