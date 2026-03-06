"""
Movies API

GET /api/v1/movies
Query params:
  page          int     default=1
  page_size     int     default=20, max=100
  year          int     filter by year_of_release
  language      str     filter by language (case-insensitive)
  sort_by       str     release_date | ratings  (default: release_date)
  sort_order    str     asc | desc              (default: desc)

Example:
  GET /api/v1/movies?year=2023&language=English&sort_by=ratings&sort_order=desc&page=2
"""

from flask import Blueprint, request
from app.utils.responses import error_response, success_response
from app.services.movie_service import list_movies

movies_bp = Blueprint("movies", __name__, url_prefix="/api/v1")

VALID_SORT_BY = {"release_date", "ratings"}
VALID_SORT_ORDER = {"asc", "desc"}


@movies_bp.route("/movies", methods=["GET"])
def get_movies():
    # Parse + validate query params
    try:
        page = int(request.args.get("page", 1))
        if page < 1:
            raise ValueError
    except (ValueError, TypeError):
        return error_response("'page' must be a positive integer.", 400)

    try:
        page_size = int(request.args.get("page_size", 20))
        if page_size < 1:
            raise ValueError
    except (ValueError, TypeError):
        return error_response("'page_size' must be a positive integer.", 400)

    year_str = request.args.get("year")
    year = None
    if year_str:
        try:
            year = int(year_str)
            if year < 1800 or year > 2100:
                raise ValueError
        except (ValueError, TypeError):
            return error_response("'year' must be a valid 4-digit year.", 400)

    language = request.args.get("language", "").strip() or None

    sort_by = request.args.get("sort_by", "release_date").strip().lower()
    if sort_by not in VALID_SORT_BY:
        return error_response(
            f"'sort_by' must be one of: {', '.join(VALID_SORT_BY)}.", 400
        )

    sort_order = request.args.get("sort_order", "desc").strip().lower()
    if sort_order not in VALID_SORT_ORDER:
        return error_response(
            f"'sort_order' must be one of: {', '.join(VALID_SORT_ORDER)}.", 400
        )

    result = list_movies(
        page=page,
        page_size=page_size,
        year_of_release=year,
        language=language,
        sort_by=sort_by,
        sort_order=sort_order,
    )

    return success_response(result)
