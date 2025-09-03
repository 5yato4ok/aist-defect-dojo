import logging
from typing import Optional
from django.db import transaction
from celery import current_app
from celery import states
from celery.result import AsyncResult

from .models import AISTPipeline, AISTStatus


class DatabaseLogHandler(logging.Handler):
    """
    Logging handler that writes log records into the AISTPipeline.logs field.

    This allows anything that logs through Python's logging to be displayed
    in the pipeline UI in near-real-time via the SSE endpoint.
    """
    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            with transaction.atomic():
                p = AISTPipeline.objects.select_for_update().get(id=self.pipeline_id)
                p.append_log(msg)
        except Exception:
            # Never break the main flow due to logging issues
            pass

def _revoke_task(task_id: Optional[str], terminate: bool = True) -> None:
    """Safely revoke a Celery task by its ID if it is still running."""
    if not task_id:
        return
    try:
        result = AsyncResult(task_id)
        if result.state not in states.READY_STATES:
            result.revoke(terminate=terminate)
    except Exception:
        # Ignore errors while revoking tasks
        pass

def stop_pipeline(pipeline: AISTPipeline) -> None:
    """Stop all Celery tasks associated with an ``AISTPipeline``.

    Revokes both the run and deduplication watcher tasks (if present)
    and updates the pipeline's status to finished.  The caller should
    save the pipeline after invoking this function.
    """
    # Revoke both the main run task and any scheduled deduplication
    # watcher.  Clearing the task identifiers afterwards helps avoid
    # attempting to revoke them again if stop is called twice.
    run_id = getattr(pipeline, "run_task_id", None)
    watch_id = getattr(pipeline, "watch_dedup_task_id", None)
    _revoke_task(run_id)
    _revoke_task(watch_id)
    pipeline.run_sast_task_id = None
    pipeline.watch_dedup_task_id = None
    pipeline.status = AISTStatus.FINISHED
