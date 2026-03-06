"""
CSV ingestion service.

Design decisions for 1GB file constraint:
- Stream file line-by-line, never load full content into memory.
- Batch inserts (bulk_write) to amortize MongoDB round-trip cost.
- Track job progress in MongoDB for async status polling.
- Ordered=False on bulk_write so one bad doc doesn't halt the batch.
- Skip + count invalid rows; report them in job result.
"""

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Generator
from bson import ObjectId
from pymongo import InsertOne

from app.core.database import get_db
from app.models.movie import build_movie_doc

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000  # rows per bulk_write call


def _row_generator(file_stream) -> Generator[dict, None, None]:
    """
    Wraps a binary file stream into a csv.DictReader generator.
    Handles UTF-8 with BOM.
    """
    text_stream = io.TextIOWrapper(file_stream, encoding="utf-8-sig", errors="replace")
    reader = csv.DictReader(text_stream)
    for row in reader:
        yield row


def process_csv_sync(file_stream, filename: str, job_id: str = None) -> dict:
    """
    Blocking CSV processor. Called directly for small files or from Celery worker.
    Returns job result dict.
    """
    db = get_db()
    job_id = job_id or str(ObjectId())
    job = {
        "job_id": job_id,
        "filename": filename,
        "status": "processing",
        "total_rows": 0,
        "inserted": 0,
        "skipped": 0,
        "errors": [],
        "started_at": datetime.now(timezone.utc),
        "completed_at": None,
    }
    db["upload_jobs"].insert_one(job)

    batch = []
    total = 0
    inserted = 0
    skipped = 0

    try:
        for row in _row_generator(file_stream):
            total += 1
            doc = build_movie_doc(row)
            if doc is None:
                skipped += 1
                if skipped <= 100:  # cap error log size
                    job["errors"].append({
                        "row": total,
                        "data": {k: v for k, v in list(row.items())[:5]},  # truncate
                        "reason": "validation_failed",
                    })
                continue

            batch.append(InsertOne(doc))

            if len(batch) >= BATCH_SIZE:
                result = db["movies"].bulk_write(batch, ordered=False)
                inserted += result.inserted_count
                batch = []

        # flush remaining
        if batch:
            result = db["movies"].bulk_write(batch, ordered=False)
            inserted += result.inserted_count

        status = "completed"

    except Exception as e:
        logger.exception("CSV processing failed for job %s", job_id)
        status = "failed"
        job["errors"].append({"reason": str(e)})

    db["upload_jobs"].update_one(
        {"job_id": job_id},
        {
            "$set": {
                "status": status,
                "total_rows": total,
                "inserted": inserted,
                "skipped": skipped,
                "errors": job["errors"],
                "completed_at": datetime.now(timezone.utc),
            }
        },
    )

    return {
        "job_id": job_id,
        "status": status,
        "total_rows": total,
        "inserted": inserted,
        "skipped": skipped,
    }


def get_job_status(job_id: str) -> dict | None:
    db = get_db()
    doc = db["upload_jobs"].find_one({"job_id": job_id})
    if not doc:
        return None
    doc["_id"] = str(doc["_id"])
    if doc.get("started_at"):
        doc["started_at"] = doc["started_at"].isoformat()
    if doc.get("completed_at"):
        doc["completed_at"] = doc["completed_at"].isoformat()
    return doc
