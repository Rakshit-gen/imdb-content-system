"""
Upload API

POST /api/v1/upload
  - Accepts multipart/form-data with 'file' field (CSV)
  - Files < SIZE_SYNC_THRESHOLD processed synchronously (returns full result)
  - Files >= SIZE_SYNC_THRESHOLD offloaded to Celery (returns job_id for polling)

GET /api/v1/upload/<job_id>
  - Poll job status
"""

import os
import tempfile
import logging

from flask import Blueprint, request
from app.utils.responses import error_response, success_response
from app.services.csv_service import process_csv_sync, get_job_status

logger = logging.getLogger(__name__)

upload_bp = Blueprint("upload", __name__, url_prefix="/api/v1")

ALLOWED_EXTENSIONS = {"csv"}
SIZE_SYNC_THRESHOLD = 10 * 1024 * 1024  # 10MB: sync below, async above


def _is_csv(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@upload_bp.route("/upload", methods=["POST"])
def upload_csv():
    if "file" not in request.files:
        return error_response("Missing 'file' field in multipart form data.", 400)

    file = request.files["file"]

    if not file.filename:
        return error_response("No file selected.", 400)

    if not _is_csv(file.filename):
        return error_response("Only CSV files are accepted.", 400)

    filename = file.filename

    # Check content-length hint if available
    content_length = request.content_length or 0

    if content_length >= SIZE_SYNC_THRESHOLD:
        # Save to temp file, dispatch to Celery
        try:
            from app.services.celery_tasks import process_csv_task
        except ImportError:
            # Celery not configured — fall back to sync
            logger.warning("Celery unavailable, processing synchronously")
            result = process_csv_sync(file.stream, filename)
            return success_response(result, 202)

        upload_dir = os.environ.get("UPLOAD_TMP_DIR", "/tmp/uploads")
        os.makedirs(upload_dir, exist_ok=True)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", dir=upload_dir)
        try:
            file.save(tmp.name)
        except Exception as e:
            os.unlink(tmp.name)
            return error_response("Failed to save uploaded file.", 500)

        task = process_csv_task.delay(tmp.name, filename)
        return success_response(
            {
                "message": "Large file accepted for async processing.",
                "job_id": task.id,
                "status_url": f"/api/v1/upload/{task.id}",
            },
            202,
        )

    # Sync path for small files
    try:
        result = process_csv_sync(file.stream, filename)
    except Exception as e:
        logger.exception("Sync CSV processing failed")
        return error_response("CSV processing failed.", 500)

    return success_response(result, 201)


@upload_bp.route("/upload/<job_id>", methods=["GET"])
def get_upload_status(job_id: str):
    job = get_job_status(job_id)
    if not job:
        return error_response("Job not found.", 404)
    return success_response(job)
