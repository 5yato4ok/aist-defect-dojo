from __future__ import annotations
import logging

from django.db import transaction
from celery import current_app
from celery import states
from celery.result import AsyncResult

from .models import AISTPipeline, AISTStatus

import os
import socket
from typing import Optional
from urllib.parse import urljoin

from django.conf import settings
from django.contrib.sites.models import Site
from django.core.exceptions import ImproperlyConfigured
from django.http import HttpRequest
from django.urls import reverse


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

def _best_effort_outbound_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # не отправляем трафик, только получаем выбранный ОС исходящий интерфейс
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


def get_public_base_url(request: Optional[HttpRequest] = None) -> str:
    """
    Generic resolver of base url. Order:
    1) settings.PUBLIC_BASE_URL
    2) request.build_absolute_uri('/')
    3) Django Sites: https://<Site.domain>
    4) DEFECTDOJO_SERVICE_HOST + DEFECTDOJO_SERVICE_PORT
    5) Auto IP: http://<outbound_ip>:<port>
    """
    # 1) Static setup
    base = getattr(settings, "PUBLIC_BASE_URL", None)
    if base:
        return base.rstrip("/")

    # 2) If there is request build from it
    if request is not None:
        return request.build_absolute_uri("/").rstrip("/")

    # 3) Django Sites — domen is stored in DB
    try:
        site = Site.objects.get_current()
        domain = site.domain.strip().rstrip("/")
        if domain:
            scheme = "https" if getattr(settings, "SECURE_SSL_REDIRECT", False) else "http"
            return f"{scheme}://{domain}"
    except Exception:
        pass

    # 4) Internal DNS/host  (applicable for Docker/K8s, when n8n and Dojo in one network)
    svc_host = os.getenv("DEFECTDOJO_SERVICE_HOST")
    svc_port = os.getenv("DEFECTDOJO_SERVICE_PORT", "8000")
    if svc_host:
        scheme = os.getenv("DEFECTDOJO_SERVICE_SCHEME", "http")
        return f"{scheme}://{svc_host}:{svc_port}".rstrip(":/")

    # 5) Check outcoming IP
    ip = _best_effort_outbound_ip()
    port = getattr(settings, "AIST_PUBLIC_FALLBACK_PORT", 8000)
    return f"http://{ip}:{port}"


def build_callback_url(pipeline_id: str, request: Optional[HttpRequest] = None) -> str:
    base = get_public_base_url(request=request)
    path = reverse("pipeline_callback", kwargs={"id": str(pipeline_id)})
    return urljoin(base + "/", path.lstrip("/"))
