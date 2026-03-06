import pytest
from app.models.movie import build_movie_doc


def test_valid_row():
    doc = build_movie_doc({
        "title": "Test Movie",
        "release_date": "2023-05-15",
        "language": "English",
        "ratings": "8.5",
    })
    assert doc is not None
    assert doc["title"] == "Test Movie"
    assert doc["year_of_release"] == 2023
    assert doc["ratings"] == 8.5


def test_missing_title_returns_none():
    doc = build_movie_doc({
        "title": "",
        "release_date": "2023-01-01",
        "language": "English",
        "ratings": "7.0",
    })
    assert doc is None


def test_invalid_date_stored_as_none():
    doc = build_movie_doc({
        "title": "No Date Movie",
        "release_date": "not-a-date",
        "language": "English",
        "ratings": "6.0",
    })
    assert doc is not None
    assert doc["release_date"] is None
    assert doc["year_of_release"] is None


def test_invalid_rating_stored_as_none():
    doc = build_movie_doc({
        "title": "Bad Rating Movie",
        "release_date": "2022-01-01",
        "language": "English",
        "ratings": "N/A",
    })
    assert doc is not None
    assert doc["ratings"] is None


def test_multiple_date_formats():
    formats = [
        ("2023-05-15", 2023),
        ("15-05-2023", 2023),
        ("05/15/2023", 2023),
        ("2023/05/15", 2023),
    ]
    for date_str, expected_year in formats:
        doc = build_movie_doc({
            "title": "T",
            "release_date": date_str,
            "language": "EN",
            "ratings": "7",
        })
        assert doc is not None, f"Failed for {date_str}"
        assert doc["year_of_release"] == expected_year, f"Wrong year for {date_str}"


def test_extra_columns_stored():
    doc = build_movie_doc({
        "title": "Extra Movie",
        "release_date": "2022-01-01",
        "language": "English",
        "ratings": "7.0",
        "director": "Nolan",
        "genre": "Thriller",
    })
    assert doc is not None
    assert "director" in doc.get("extra", {})
    assert "genre" in doc.get("extra", {})


def test_language_stored_lowercase():
    doc = build_movie_doc({
        "title": "Test",
        "release_date": "2023-01-01",
        "language": "English",
        "ratings": "7.0",
    })
    assert doc["language"] == "english"


def test_vote_average_mapped_to_ratings():
    doc = build_movie_doc({
        "title": "Test",
        "release_date": "2023-01-01",
        "vote_average": "8.3",
    })
    assert doc is not None
    assert doc["ratings"] == 8.3


def test_original_language_fallback():
    doc = build_movie_doc({
        "title": "Test",
        "release_date": "2023-01-01",
        "original_language": "FR",
        "ratings": "7.0",
    })
    assert doc is not None
    assert doc["language"] == "fr"


def test_created_at_set():
    doc = build_movie_doc({
        "title": "Test",
        "release_date": "2023-01-01",
        "language": "en",
        "ratings": "7.0",
    })
    assert doc is not None
    assert "created_at" in doc


def test_zero_rating_stored():
    doc = build_movie_doc({
        "title": "Test",
        "release_date": "2023-01-01",
        "language": "en",
        "ratings": "0",
    })
    assert doc is not None
    assert doc["ratings"] == 0.0


def test_empty_release_date():
    doc = build_movie_doc({
        "title": "Test",
        "release_date": "",
        "language": "en",
        "ratings": "5.0",
    })
    assert doc is not None
    assert doc["release_date"] is None
    assert doc["year_of_release"] is None
