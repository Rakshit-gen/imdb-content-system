# IMDb Content Upload & Review System

A backend system that lets the IMDb content team upload movie data via CSV files and query it through a clean REST API. Built with Flask and MongoDB.

## What it does

- Upload CSV files containing movie data (handles files up to 1GB)
- Browse movies with pagination, filtering by year/language, and sorting by release date or ratings
- Large files (>10MB) are processed in the background so uploads don't hang

## Quick start

### Using Docker (recommended)

This is the easiest way to get everything running. You'll need Docker and Docker Compose installed.

```bash
# 1. Clone the repo
git clone <repo-url>
cd imdb-content-system

# 2. Create your env file
cp .env.example .env

# 3. Start everything
docker-compose up --build
```

That's it. The API will be available at `http://localhost:5001`. MongoDB, Redis, and the Celery worker all start automatically.

To stop everything:
```bash
docker-compose down
```

### Running locally (without Docker)

You'll need Python 3.11+, MongoDB 7, and Redis 7 running on your machine.

```bash
# 1. Set up a virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Create your env file and update the URIs if needed
cp .env.example .env

# 3. Start the API server
flask --app wsgi run --port 5000

# 4. In a separate terminal, start the Celery worker
#    (only needed if you're uploading files larger than 10MB)
celery -A app.services.celery_tasks.celery_app worker --loglevel=info
```

## Running tests

Tests use `mongomock` so you don't need a running MongoDB instance.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pytest -v
```

You should see all 47 tests pass.

## How to use the API

### Upload a CSV

```bash
curl -X POST http://localhost:5001/api/v1/upload \
  -F "file=@movies_data_assignment.csv"
```

For small files (<10MB), you'll get the result right away:

```json
{
  "job_id": "abc123",
  "status": "completed",
  "total_rows": 1000,
  "inserted": 998,
  "skipped": 2
}
```

For larger files, the server accepts the file and processes it in the background:

```json
{
  "message": "Large file accepted for async processing.",
  "job_id": "abc123",
  "status_url": "/api/v1/upload/abc123"
}
```

You can check on the progress by polling the status URL:

```bash
curl http://localhost:5001/api/v1/upload/abc123
```

### Browse movies

Get a paginated list of movies:

```bash
curl "http://localhost:5001/api/v1/movies"
```

Filter and sort:

```bash
# English movies from 2023, sorted by highest rating
curl "http://localhost:5001/api/v1/movies?year=2023&language=en&sort_by=ratings&sort_order=desc"

# Page 2, 50 results per page
curl "http://localhost:5001/api/v1/movies?page=2&page_size=50"
```

Available query parameters:

| Parameter  | Type   | Default        | Notes                                  |
|------------|--------|----------------|----------------------------------------|
| page       | int    | 1              |                                        |
| page_size  | int    | 20             | Max 100                                |
| year       | int    | -              | Filter by year of release              |
| language   | string | -              | Filter by language (case-insensitive)  |
| sort_by    | string | release_date   | `release_date` or `ratings`            |
| sort_order | string | desc           | `asc` or `desc`                        |

Response shape:

```json
{
  "data": [
    {
      "_id": "...",
      "title": "Inception",
      "release_date": "2010-07-16T00:00:00",
      "language": "en",
      "ratings": 8.8,
      "year_of_release": 2010
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total": 45428,
    "total_pages": 2272,
    "has_next": true,
    "has_prev": false
  },
  "filters": { "year_of_release": null, "language": null },
  "sort": { "by": "release_date", "order": "desc" }
}
```

### Health check

```bash
curl http://localhost:5001/health
# {"status": "ok"}
```

## Sample data

Download the sample CSV here: [movies_data_assignment.csv](https://drive.google.com/file/d/1xyeqXiCn4q9fufFGLaMIhyt-U7LGfeVY/view)

## CSV format

The system expects a CSV with headers. These columns are recognized:

- `title` (required)
- `release_date` — supports `YYYY-MM-DD`, `DD-MM-YYYY`, `MM/DD/YYYY`, `YYYY/MM/DD`
- `language` or `original_language`
- `ratings` or `vote_average`

Any extra columns in your CSV are stored in an `extra` field on each movie document, so nothing gets lost.

Rows with a missing or empty title are skipped. Everything else is handled gracefully — bad dates become `null`, bad ratings become `null`, etc.

## Testing with Postman

There's a Postman collection included in the repo (`postman_collection.json`). Import it into Postman and set the `base_url` variable to `http://localhost:5001`.

## Project structure

```
app/
  api/          # Route handlers (upload, movies)
  core/         # Config, database connection, indexes
  models/       # Movie schema and validation (Marshmallow)
  services/     # Business logic (CSV processing, movie queries, Celery tasks)
  utils/        # Response helpers
tests/          # pytest tests (mongomock, no live DB needed)
```

## Design decisions worth mentioning

**Handling large CSVs**: The CSV is streamed line-by-line using `csv.DictReader` and inserted in batches of 1000 via `bulk_write`. The file never gets loaded into memory all at once, so even a 1GB CSV won't blow up the server.

**Sync vs async uploads**: Files under 10MB are processed during the request. Larger files get saved to disk and handed off to a Celery worker so the HTTP request can return immediately. The client gets a `job_id` to poll for status.

**Database indexes**: Compound indexes are set up for every filter+sort combination the API supports (year+release_date, year+ratings, language+release_date, language+ratings — both ascending and descending). This means queries hit the index instead of scanning the whole collection.

**Language handling**: Languages are stored in lowercase so filtering is a simple equality check (no regex, no collation overhead). The API accepts any casing from the user and normalizes it.
