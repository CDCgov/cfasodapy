import pytest

from cfasodapy import Query


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
    ["offset", "limit", "expected_start", "expected_end"],
    [
        # assuming 10 records...
        # cover the whole dataset
        (0, None, 0, 9),
        # get only 1 records
        (0, 1, 0, 0),
        # get only the first 2 records
        (0, 2, 0, 1),
        # skip 1 record, go to the end
        (1, None, 1, 9),
        # skip 5 records, try to go past the end, go to the end
        (5, 100, 5, 9),
        # skip many records; start & end at the end
        (100, None, 9, 9),
    ],
)
def test_start_end(monkeypatch, offset, limit, expected_start, expected_end):
    monkeypatch.setattr(Query, "_n_where_records", 10)
    q = Query(domain="data.cdc.gov", id="abc123", limit=limit, offset=offset)
    assert q._start_record == expected_start
    assert q._end_record == expected_end


def test_pages(monkeypatch):
    def mock_get_records(_, start, end):
        return list(range(start, end))

    monkeypatch.setattr(Query, "_get_records", mock_get_records)
    monkeypatch.setattr(Query, "_n_where_records", 100)

    q = Query(domain="data.cdc.gov", id="abc123", offset=0, limit=None)
    pages = list(q.get_pages(page_size=50))
    assert len(pages) == 2
    assert pages[0][0] == 0
    assert pages[0][-1] == 49
    assert pages[1][0] == 50
    assert pages[1][-1] == 99
