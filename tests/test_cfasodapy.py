from cfasodapy import Query


def test_build_url():
    """
    Test the _build_url method of the Query class.
    """
    domain = "data.cityofnewyork.us"
    id = "abc123"
    clauses = {"where": "x='foo'"}
    expected_url = "https://data.cityofnewyork.us/resource/abc123.json?$where=x='foo'"

    assert Query._build_url(domain=domain, id=id, clauses=clauses) == expected_url
