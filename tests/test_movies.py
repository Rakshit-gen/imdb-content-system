import io
import pytest
from datetime import datetime


SEED_CSV = b"""title,release_date,language,ratings
Movie A,2022-01-15,English,8.5
Movie B,2022-06-20,English,6.2
Movie C,2021-03-10,Korean,9.1
Movie D,2021-11-05,Korean,7.3
Movie E,2020-07-22,French,8.0
Movie F,2023-02-28,English,5.5
"""


def _seed(client):
    data = {"file": (io.BytesIO(SEED_CSV), "seed.csv")}
    client.post("/api/v1/upload", data=data, content_type="multipart/form-data")


def test_list_movies_basic(client):
    _seed(client)
    resp = client.get("/api/v1/movies")
    assert resp.status_code == 200
    body = resp.get_json()
    assert "data" in body
    assert "pagination" in body
    assert len(body["data"]) == 6


def test_filter_by_year(client):
    _seed(client)
    resp = client.get("/api/v1/movies?year=2022")
    body = resp.get_json()
    assert body["pagination"]["total"] == 2
    years = {m["year_of_release"] for m in body["data"]}
    assert years == {2022}


def test_filter_by_language(client):
    _seed(client)
    resp = client.get("/api/v1/movies?language=Korean")
    body = resp.get_json()
    assert body["pagination"]["total"] == 2
    langs = {m["language"] for m in body["data"]}
    assert langs == {"korean"}


def test_filter_language_case_insensitive(client):
    _seed(client)
    resp = client.get("/api/v1/movies?language=english")
    body = resp.get_json()
    assert body["pagination"]["total"] == 3


def test_filter_combined(client):
    _seed(client)
    resp = client.get("/api/v1/movies?year=2021&language=Korean")
    body = resp.get_json()
    assert body["pagination"]["total"] == 2


def test_sort_by_ratings_desc(client):
    _seed(client)
    resp = client.get("/api/v1/movies?sort_by=ratings&sort_order=desc")
    body = resp.get_json()
    ratings = [m["ratings"] for m in body["data"] if m.get("ratings") is not None]
    assert ratings == sorted(ratings, reverse=True)


def test_sort_by_ratings_asc(client):
    _seed(client)
    resp = client.get("/api/v1/movies?sort_by=ratings&sort_order=asc")
    body = resp.get_json()
    ratings = [m["ratings"] for m in body["data"] if m.get("ratings") is not None]
    assert ratings == sorted(ratings)


def test_sort_by_release_date_asc(client):
    _seed(client)
    resp = client.get("/api/v1/movies?sort_by=release_date&sort_order=asc")
    body = resp.get_json()
    dates = [m["release_date"] for m in body["data"] if m.get("release_date")]
    assert dates == sorted(dates)


def test_sort_by_release_date_desc(client):
    _seed(client)
    resp = client.get("/api/v1/movies?sort_by=release_date&sort_order=desc")
    body = resp.get_json()
    dates = [m["release_date"] for m in body["data"] if m.get("release_date")]
    assert dates == sorted(dates, reverse=True)


def test_pagination(client):
    _seed(client)
    resp = client.get("/api/v1/movies?page=1&page_size=2")
    body = resp.get_json()
    assert len(body["data"]) == 2
    assert body["pagination"]["total_pages"] == 3
    assert body["pagination"]["has_next"] is True
    assert body["pagination"]["has_prev"] is False


def test_pagination_last_page(client):
    _seed(client)
    resp = client.get("/api/v1/movies?page=3&page_size=2")
    body = resp.get_json()
    assert body["pagination"]["has_next"] is False
    assert body["pagination"]["has_prev"] is True


def test_page_size_capped_at_max(client):
    _seed(client)
    resp = client.get("/api/v1/movies?page_size=500")
    body = resp.get_json()
    assert body["pagination"]["page_size"] == 100


def test_invalid_page(client):
    resp = client.get("/api/v1/movies?page=abc")
    assert resp.status_code == 400


def test_invalid_page_negative(client):
    resp = client.get("/api/v1/movies?page=-1")
    assert resp.status_code == 400


def test_invalid_page_size_zero(client):
    resp = client.get("/api/v1/movies?page_size=0")
    assert resp.status_code == 400


def test_invalid_page_size_negative(client):
    resp = client.get("/api/v1/movies?page_size=-5")
    assert resp.status_code == 400


def test_invalid_sort_by(client):
    resp = client.get("/api/v1/movies?sort_by=invalid_field")
    assert resp.status_code == 400


def test_invalid_sort_order(client):
    resp = client.get("/api/v1/movies?sort_order=random")
    assert resp.status_code == 400


def test_invalid_year(client):
    resp = client.get("/api/v1/movies?year=9999")
    assert resp.status_code == 400


def test_empty_result(client):
    _seed(client)
    resp = client.get("/api/v1/movies?year=1900")
    body = resp.get_json()
    assert body["pagination"]["total"] == 0
    assert body["pagination"]["total_pages"] == 0
    assert body["data"] == []


def test_large_page_number(client):
    _seed(client)
    resp = client.get("/api/v1/movies?page=999999")
    body = resp.get_json()
    assert resp.status_code == 200
    assert body["data"] == []
    assert body["pagination"]["has_next"] is False


def test_response_structure(client):
    _seed(client)
    resp = client.get("/api/v1/movies")
    body = resp.get_json()
    assert "data" in body
    assert "pagination" in body
    assert "filters" in body
    assert "sort" in body
    pagination = body["pagination"]
    assert all(k in pagination for k in ["page", "page_size", "total", "total_pages", "has_next", "has_prev"])
