"""
Movie listing service.

Cursor-based pagination via skip+limit.
For very large collections, use cursor-based (last_id) pagination instead.
This implementation uses skip/limit with a capped PAGE_SIZE_MAX to stay practical.
"""

from flask import current_app
from app.core.database import get_db
from app.core.config import get_config
from pymongo import ASCENDING, DESCENDING

SORT_FIELD_MAP = {
    "release_date": "release_date",
    "ratings": "ratings",
}

SORT_ORDER_MAP = {
    "asc": ASCENDING,
    "desc": DESCENDING,
}

PAGE_SIZE_DEFAULT = 20
PAGE_SIZE_MAX = 100


def list_movies(
    page: int = 1,
    page_size: int = None,
    year_of_release: int = None,
    language: str = None,
    sort_by: str = "release_date",
    sort_order: str = "desc",
) -> dict:
    db = get_db()

    try:
        ps_default = current_app.config.get("PAGE_SIZE_DEFAULT", PAGE_SIZE_DEFAULT)
        ps_max = current_app.config.get("PAGE_SIZE_MAX", PAGE_SIZE_MAX)
    except RuntimeError:
        ps_default = PAGE_SIZE_DEFAULT
        ps_max = PAGE_SIZE_MAX

    page = max(1, page)
    page_size = max(1, min(
        int(page_size) if page_size else ps_default,
        ps_max,
    ))

    # Build filter
    query = {}
    if year_of_release is not None:
        try:
            query["year_of_release"] = int(year_of_release)
        except (ValueError, TypeError):
            pass
    if language:
        query["language"] = language.strip().lower()

    # Resolve sort
    sort_field = SORT_FIELD_MAP.get(sort_by, "release_date")
    sort_dir = SORT_ORDER_MAP.get(sort_order, DESCENDING)

    col = db["movies"]
    total = col.count_documents(query)

    skip = (page - 1) * page_size
    cursor = (
        col.find(query, {"extra": 0, "created_at": 0})
        .sort(sort_field, sort_dir)
        .skip(skip)
        .limit(page_size)
    )

    movies = []
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        if doc.get("release_date"):
            doc["release_date"] = doc["release_date"].isoformat() if hasattr(doc["release_date"], "isoformat") else doc["release_date"]
        movies.append(doc)

    total_pages = (total + page_size - 1) // page_size if total > 0 else 0

    return {
        "data": movies,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "has_next": skip + page_size < total,
            "has_prev": page > 1,
        },
        "filters": {
            "year_of_release": year_of_release,
            "language": language,
        },
        "sort": {
            "by": sort_by,
            "order": sort_order,
        },
    }
