import threading

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure
from app.core.config import get_config

_client = None
_db = None
_lock = threading.Lock()


def get_db():
    global _client, _db
    if _db is None:
        with _lock:
            if _db is None:
                cfg = get_config()
                _client = MongoClient(cfg.MONGO_URI)
                _db = _client.get_default_database()
                _ensure_indexes(_db)
    return _db


def _ensure_indexes(db):
    """
    Indexes designed for the three query patterns:
    1. Filter by year_of_release (equality)
    2. Filter by language (equality)
    3. Sort by release_date or ratings
    Compound index covers filter + sort together.
    """
    col = db["movies"]

    # Compound: filter by year + sort by release_date
    col.create_index(
        [("year_of_release", ASCENDING), ("release_date", ASCENDING)],
        name="idx_year_release_date_asc",
    )
    col.create_index(
        [("year_of_release", ASCENDING), ("release_date", DESCENDING)],
        name="idx_year_release_date_desc",
    )

    # Compound: filter by year + sort by ratings
    col.create_index(
        [("year_of_release", ASCENDING), ("ratings", ASCENDING)],
        name="idx_year_ratings_asc",
    )
    col.create_index(
        [("year_of_release", ASCENDING), ("ratings", DESCENDING)],
        name="idx_year_ratings_desc",
    )

    # Compound: filter by language + sort by release_date / ratings
    col.create_index(
        [("language", ASCENDING), ("release_date", ASCENDING)],
        name="idx_lang_release_date_asc",
    )
    col.create_index(
        [("language", ASCENDING), ("release_date", DESCENDING)],
        name="idx_lang_release_date_desc",
    )
    col.create_index(
        [("language", ASCENDING), ("ratings", ASCENDING)],
        name="idx_lang_ratings_asc",
    )
    col.create_index(
        [("language", ASCENDING), ("ratings", DESCENDING)],
        name="idx_lang_ratings_desc",
    )

    # Upload job tracking
    db["upload_jobs"].create_index([("job_id", ASCENDING)], name="idx_job_id", unique=True)
    db["upload_jobs"].create_index([("status", ASCENDING)], name="idx_job_status")


def close_db():
    global _client, _db
    if _client:
        _client.close()
        _client = None
        _db = None
