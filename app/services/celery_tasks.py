"""
Celery task for async CSV processing.

For 1GB files, the upload API saves the file to a temp path
and offloads processing to a Celery worker, returning job_id immediately.
This prevents HTTP timeout and server memory exhaustion.
"""

import os
import logging
from celery import Celery
from app.core.config import get_config

logger = logging.getLogger(__name__)

cfg = get_config()
celery_app = Celery(
    "imdb_tasks",
    broker=cfg.CELERY_BROKER_URL,
    backend=cfg.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_acks_late=True,  # re-queue on worker crash
    worker_prefetch_multiplier=1,  # one task per worker (memory safety)
)


@celery_app.task(name="tasks.process_csv", bind=True, max_retries=2)
def process_csv_task(self, file_path: str, filename: str) -> dict:
    from app.services.csv_service import process_csv_sync

    try:
        with open(file_path, "rb") as f:
            result = process_csv_sync(f, filename, job_id=self.request.id)
        return result
    except Exception as exc:
        logger.exception("Task failed for file %s", file_path)
        raise self.retry(exc=exc, countdown=30)
    finally:
        # cleanup temp file
        try:
            os.unlink(file_path)
        except OSError:
            pass
