import logging
from django.db import transaction
from .models import AISTPipeline

class DatabaseLogHandler(logging.Handler):
    """Append log records into SASTPipeline.logs (best-effort)."""
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
            # swallow errors to avoid breaking main flow
            pass
