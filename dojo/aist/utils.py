from __future__ import annotations
import logging

from django.db import transaction
from celery import current_app
from celery import states
from celery.result import AsyncResult

from .models import AISTPipeline, AISTStatus

import os
import sys
import ipaddress
import socket
from urllib.parse import urlsplit, urlunsplit
from typing import Optional
from urllib.parse import urljoin

from django.conf import settings
from django.contrib.sites.models import Site
from django.core.exceptions import ImproperlyConfigured
from django.http import HttpRequest
from functools import lru_cache
from django.urls import reverse
from urllib.parse import urlencode

def _import_sast_pipeline_package():
    pipeline_path = getattr(settings, "AIST_PIPELINE_CODE_PATH", None)
    if not pipeline_path or not os.path.isdir(pipeline_path):
        raise RuntimeError(
            "SAST pipeline code path is not configured or does not exist. "
            "Please set AIST_PIPELINE_CODE_PATH."
        )
    if pipeline_path not in sys.path:
        sys.path.append(pipeline_path)

def _load_analyzers_config():
    _import_sast_pipeline_package()
    import importlib
    return importlib.import_module("pipeline.config_utils").AnalyzersConfigHelper()

def _fmt_duration(start, end):
    if not start or not end:
        return None
    total = int((end - start).total_seconds())
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def _qs_without(request, *keys):
    params = request.GET.copy()
    for k in keys:
        params.pop(k, None)
    return urlencode(params, doseq=True)

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


def _is_abs_url(value: str) -> bool:
    try:
        p = urlsplit(value)
        return bool(p.scheme and p.netloc)
    except Exception:
        return False


def _host_with_optional_port(scheme: str, host: str, port: Optional[str]) -> str:
    """Compose host[:port] with IPv6 support and default-port elision."""
    # strip scheme if accidentally included in host
    if "://" in host:
        host = urlsplit(host).netloc or host

    # Handle IPv6 literals
    try:
        ipaddress.IPv6Address(host)
        host = f"[{host}]"
    except Exception:
        pass

    # Omit default ports
    if port and not (
        (scheme == "http" and port in ("80", 80))
        or (scheme == "https" and port in ("443", 443))
    ):
        return f"{host}:{port}"
    return host


def _scheme_from_settings_or_request(request: Optional[HttpRequest]) -> str:
    """
    Decide scheme respecting SECURE_SSL_REDIRECT and reverse proxy headers.
    """
    # If explicitly forced to HTTPS
    if getattr(settings, "SECURE_SSL_REDIRECT", False):
        return "https"

    # Respect proxy header if configured (e.g., Nginx/Traefik)
    # settings.SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
    hdr = getattr(settings, "SECURE_PROXY_SSL_HEADER", None)
    if request is not None and hdr:
        header_name, expected_value = hdr
        actual = request.META.get(header_name, "")
        if actual.split(",")[0].strip().lower() == expected_value.lower():
            return "https"

    # Fallback to request.is_secure() if present
    if request is not None and request.is_secure():
        return "https"

    return "http"


def _normalize_base_url(url: str) -> str:
    """Ensure we return 'scheme://host[:port]' without path/query/fragment and no trailing slash."""
    p = urlsplit(url)
    # If caller passed just a domain (e.g., 'example.com'), add scheme
    scheme = p.scheme or "http"
    netloc = p.netloc or p.path  # sometimes users pass 'example.com' (path filled)
    if not netloc:
        return ""
    return urlunsplit((scheme, netloc.strip("/"), "", "", "")).rstrip("/")


def get_public_base_url(request: Optional[HttpRequest] = None) -> str:
    """
    Generic resolver of base url. Order:
    1) settings.PUBLIC_BASE_URL (must be absolute URL)
    2) request.build_absolute_uri('/') (if request provided)
    3) Django Sites: https(s)://<Site.domain>
    4) DEFECTDOJO_SERVICE_HOST[:DEFECTDOJO_SERVICE_PORT] (e.g., in Docker/K8s)
    5) Auto IP: http://<outbound_ip>:<port>
    """
    # 1) Explicit setting
    base = getattr(settings, "PUBLIC_BASE_URL", None)
    if base:
        if not _is_abs_url(base):
            # allow legacy values like 'example.com' by normalizing
            base = "https://" + base.lstrip("/")
        return _normalize_base_url(base)

    # 2) From request (most reliable in web context)
    if request is not None:
        # Respect USE_X_FORWARDED_HOST if enabled
        # settings.USE_X_FORWARDED_HOST = True
        url = request.build_absolute_uri("/")
        # Optionally force https if SECURE_SSL_REDIRECT=True
        scheme = _scheme_from_settings_or_request(request)
        p = urlsplit(url)
        netloc = p.netloc
        final = urlunsplit((scheme, netloc, "", "", ""))
        return final.rstrip("/")

    # 3) Django Sites
    try:
        site = Site.objects.get_current()
        domain = (site.domain or "").strip().strip("/")
        if domain:
            # If admin accidentally stored with scheme/path — normalize
            candidate = domain if "://" in domain else f"https://{domain}"
            norm = _normalize_base_url(candidate)
            if norm:
                # Respect SECURE_SSL_REDIRECT
                scheme = "https" if getattr(settings, "SECURE_SSL_REDIRECT", False) else urlsplit(norm).scheme or "http"
                host = urlsplit(norm).netloc
                return urlunsplit((scheme, host, "", "", "")).rstrip("/")
    except Exception:
        pass  # fall through

    # 4) Service env (typical for Docker/K8s)
    svc_host = os.getenv("DEFECTDOJO_SERVICE_HOST", "").strip()
    if svc_host:
        scheme = os.getenv("DEFECTDOJO_SERVICE_SCHEME", "").strip().lower() or "http"
        port = os.getenv("DEFECTDOJO_SERVICE_PORT", "").strip() or None
        netloc = _host_with_optional_port(scheme, svc_host, port)
        return urlunsplit((scheme, netloc, "", "", "")).rstrip("/")

    # 5) Outbound IP best-effort (last resort; useful for local dev/docker-compose)
    try:
        ip = _best_effort_outbound_ip()  # your existing helper
    except Exception:
        ip = "127.0.0.1"
    port = str(getattr(settings, "AIST_PUBLIC_FALLBACK_PORT", 8000))
    scheme = "http"
    netloc = _host_with_optional_port(scheme, ip, port)
    return urlunsplit((scheme, netloc, "", "", "")).rstrip("/")


def build_callback_url(pipeline_id: str, request: Optional[HttpRequest] = None) -> str:
    base = get_public_base_url(request=request)
    path = reverse("dojo_aist:pipeline_callback", kwargs={"id": str(pipeline_id)})
    return urljoin(base + "/", path.lstrip("/"))
