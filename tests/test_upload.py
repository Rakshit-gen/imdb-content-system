import io
import json
import pytest


VALID_CSV = b"""title,release_date,language,ratings
The Dark Knight,2008-07-18,English,9.0
Inception,2010-07-16,English,8.8
Parasite,2019-05-30,Korean,8.6
RRR,2022-03-25,Telugu,7.8
"""

INVALID_CSV = b"""title,release_date,language,ratings
,2020-01-01,English,7.5
"""

PARTIAL_CSV = b"""title,release_date,language,ratings
Good Movie,2021-06-15,French,8.1
,bad_date,Unknown,not_a_number
Valid Film,2020-03-10,Spanish,6.5
"""

EMPTY_CSV = b"""title,release_date,language,ratings
"""

VOTE_AVERAGE_CSV = b"""title,release_date,original_language,vote_average
Test Movie,2023-01-15,en,7.5
"""


def _upload(client, content: bytes, filename="test.csv"):
    data = {"file": (io.BytesIO(content), filename)}
    return client.post(
        "/api/v1/upload",
        data=data,
        content_type="multipart/form-data",
    )


def test_upload_valid_csv(client):
    resp = _upload(client, VALID_CSV)
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["inserted"] == 4
    assert body["skipped"] == 0
    assert body["status"] == "completed"


def test_upload_no_file(client):
    resp = client.post("/api/v1/upload", data={}, content_type="multipart/form-data")
    assert resp.status_code == 400
    assert "Missing" in resp.get_json()["error"]


def test_upload_wrong_extension(client):
    data = {"file": (io.BytesIO(b"col1,col2\n1,2"), "data.txt")}
    resp = client.post("/api/v1/upload", data=data, content_type="multipart/form-data")
    assert resp.status_code == 400
    assert "CSV" in resp.get_json()["error"]


def test_upload_invalid_rows_skipped(client):
    resp = _upload(client, INVALID_CSV)
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["skipped"] == 1
    assert body["inserted"] == 0


def test_upload_partial_valid(client):
    resp = _upload(client, PARTIAL_CSV)
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["inserted"] == 2
    assert body["skipped"] == 1


def test_upload_empty_csv(client):
    resp = _upload(client, EMPTY_CSV)
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["inserted"] == 0
    assert body["total_rows"] == 0
    assert body["status"] == "completed"


def test_upload_vote_average_column(client):
    resp = _upload(client, VOTE_AVERAGE_CSV)
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["inserted"] == 1

    movies = client.get("/api/v1/movies").get_json()
    movie = movies["data"][0]
    assert movie["ratings"] == 7.5
    assert movie["language"] == "en"


def test_get_job_status_not_found(client):
    resp = client.get("/api/v1/upload/000000000000000000000000")
    assert resp.status_code == 404


def test_get_job_status_after_upload(client):
    resp = _upload(client, VALID_CSV)
    job_id = resp.get_json()["job_id"]
    status_resp = client.get(f"/api/v1/upload/{job_id}")
    assert status_resp.status_code == 200
    body = status_resp.get_json()
    assert body["status"] == "completed"
    assert body["inserted"] == 4


def test_upload_response_has_job_id(client):
    resp = _upload(client, VALID_CSV)
    body = resp.get_json()
    assert "job_id" in body
    assert "status" in body
    assert "total_rows" in body
    assert "inserted" in body
    assert "skipped" in body


def test_upload_stores_language_lowercase(client):
    resp = _upload(client, VALID_CSV)
    assert resp.status_code == 201
    movies = client.get("/api/v1/movies").get_json()
    languages = {m["language"] for m in movies["data"]}
    for lang in languages:
        assert lang == lang.lower()
