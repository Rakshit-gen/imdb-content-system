from datetime import datetime, timezone
from marshmallow import Schema, fields, validate, pre_load, ValidationError, EXCLUDE


DATE_FORMATS = ["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d"]


def _parse_date(raw: str):
    if not raw or not raw.strip():
        return None
    raw = raw.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _parse_float(raw):
    try:
        val = float(str(raw).strip())
        return round(val, 2)
    except (ValueError, TypeError):
        return None


class MovieSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    title = fields.Str(required=True, validate=validate.Length(min=1, max=500))
    release_date = fields.DateTime(allow_none=True)
    year_of_release = fields.Int(allow_none=True)
    language = fields.Str(allow_none=True)
    ratings = fields.Float(allow_none=True)
    extra = fields.Dict(load_default=dict)

    @pre_load
    def normalize(self, data, **kwargs):
        normalized = {k.strip().lower().replace(" ", "_"): v for k, v in data.items()}

        raw_date = normalized.get("release_date")
        parsed_date = _parse_date(raw_date) if raw_date else None
        if parsed_date:
            normalized["release_date"] = parsed_date.isoformat()
            normalized["year_of_release"] = parsed_date.year
        else:
            normalized["release_date"] = None
            normalized.setdefault("year_of_release", None)

        raw_rating = normalized.get("ratings") or normalized.get("rating") or normalized.get("vote_average")
        normalized["ratings"] = _parse_float(raw_rating)

        if not normalized.get("language"):
            normalized["language"] = normalized.get("original_language")

        if normalized.get("language"):
            normalized["language"] = str(normalized["language"]).strip().lower()

        core = {"title", "release_date", "year_of_release", "language", "ratings"}
        extra = {k: v for k, v in normalized.items() if k not in core}
        normalized["extra"] = extra

        return normalized


movie_schema = MovieSchema()


def build_movie_doc(row: dict):
    try:
        doc = movie_schema.load(row)
    except ValidationError:
        return None
    doc["created_at"] = datetime.now(timezone.utc)
    return doc
