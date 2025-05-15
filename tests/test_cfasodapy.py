import pytest

from cfasodapy import Query


def test_build_url():
    """
    Test the _build_url method of the Query class.
    """
    domain = "data.cdc.gov"
    id = "abc123"
    expected_url = "https://data.cdc.gov/resource/abc123.json"

    assert Query._build_url(domain=domain, id=id) == expected_url


def test_validate_clauses():
    with pytest.raises(RuntimeError, match="select"):
        Query._validate_clauses(clauses={"select": "value"})
