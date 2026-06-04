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


def test_pages_even(monkeypatch):
    n_records = 100

    def mock_get_page(_, offset, limit):
        return list(range(offset, min(n_records, offset + limit)))

    monkeypatch.setattr(Query, "_get_page", mock_get_page)
    monkeypatch.setattr(Query, "n_records", n_records)

    q = Query(domain="data.cdc.gov", id="abc123", offset=0, limit=None)
    pages = list(q.get_pages(page_size=50))
    assert len(pages) == 2
    assert pages[0][0] == 0
    assert pages[0][-1] == 49
    assert pages[1][0] == 50
    assert pages[1][-1] == 99


def test_pages_uneven(monkeypatch):
    n_records = 100
    page_size = 75

    def mock_get_page(_, offset, limit):
        return list(range(offset, min(n_records, offset + limit)))

    monkeypatch.setattr(Query, "_get_page", mock_get_page)
    monkeypatch.setattr(Query, "n_records", n_records)

    q = Query(domain="data.cdc.gov", id="abc123", offset=0, limit=None)
    pages = list(q.get_pages(page_size=page_size))
    assert len(pages) == 2
    assert pages[0][0] == 0
    assert pages[0][-1] == page_size - 1
    assert pages[1][0] == page_size
    assert pages[1][-1] == n_records - 1
