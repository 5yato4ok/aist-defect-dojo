from __future__ import annotations

from .utils import build_callback_url, _import_sast_pipeline_package

_import_sast_pipeline_package()

import os
import json
import requests
import sys
import time
import logging
from django.utils import timezone
from typing import Any, Dict, List

from celery import shared_task, current_app, chord
from django.conf import settings

from .models import AISTPipeline, AISTStatus
from typing import Any, Dict, Iterable
from .internal_upload import upload_results_internal
from .logging_transport import get_redis, STREAM_KEY
from django.db.models.functions import Concat
from collections import defaultdict
from django.db.models import F, Value

from pipeline.project_builder import configure_project_run_analyses  # type: ignore
from pipeline.config_utils import AnalyzersConfigHelper  # type: ignore
from pipeline.defect_dojo.repo_info import read_repo_params  # type: ignore
from django.db import transaction, OperationalError
from celery.exceptions import Ignore
from dojo.models import Test, Finding, DojoMeta

import requests

from typing import Optional
from celery import shared_task  # type: ignore
from .models import TestDeduplicationProgress
from .logging_transport import _install_db_logging

@shared_task(bind=True)
def run_sast_pipeline(self, pipeline_id: str, params: Dict[str, Any]) -> None:
    """
    Execute a SAST pipeline asynchronously.

    This task coordinates the SAST pipeline by invoking the configure
    and upload functions provided by the external ``sast-pipeline``
    package. All progress is recorded in the database so that
    connected clients can observe status changes and log output in real time.

    :param pipeline_id: Primary key of the :class:`AISTPipeline` instance.
    :param params: Dictionary of parameters collected from the form.
    """

    def get_param(key: str, default: Any) -> Any:
        value = params.get(key)
        return value if value not in (None, "", []) else default

    log_level = get_param("log_level", "INFO")
    logger = _install_db_logging(pipeline_id, log_level)

    pipeline = None

    try:
        with transaction.atomic():
            pipeline = (
                AISTPipeline.objects
                .select_for_update()
                .select_related("project")
                .get(id=pipeline_id)
            )

            # protection from secondary launch
            if pipeline.status != AISTStatus.FINISHED:
                logger.info("Pipeline already in progress; skipping duplicate start.")
                return

            pipeline.status = AISTStatus.SAST_LAUNCHED
            pipeline.started = timezone.now()
            pipeline.save(update_fields=["status", "started", "updated"])


            project = pipeline.project
            project_name = project.product.name if project else None
            project_version = pipeline.project_version.version if pipeline.project_version else None
            project_supported_languages = project.supported_languages if project else []

        analyzers_helper = AnalyzersConfigHelper()

        aist_path = getattr(settings, "AIST_OUTPUT_PATH", os.path.join("/tmp", "aist", "output"))
        output_dir =  os.path.join(aist_path, project_name or "project")
        languages = get_param("languages", [])
        if isinstance(languages, str):
            languages = [languages]
        languages = list(set(languages + project_supported_languages))
        project_version = get_param("project_version", project_version)
        rebuild_images = bool(get_param("rebuild_images", False))
        analyzers = get_param("analyzers", [])
        time_class_level = get_param("time_class_level", "")
        dojo_product_name = get_param("dojo_product_name", project_name or None)

        pipeline_path = getattr(settings, "AIST_PIPELINE_CODE_PATH", None)
        script_path = get_param("script_path", None)
        script_path = os.path.join(pipeline_path, script_path)
        if not os.path.isfile(script_path):
            raise RuntimeError("Incorrrect script path for AIST pipeline.")
        dockerfile_path = os.path.join(pipeline_path, "Dockerfiles", "builder", "Dockerfile")
        if not os.path.isfile(dockerfile_path):
            raise RuntimeError("Dockerfile does not exist")

        project_build_path = getattr(settings, "AIST_PROJECTS_BUILD_DIR", None)
        if not project_build_path:
            raise RuntimeError("Project build path for AIST is not setup")

        logger.info("Starting configure_project_run_analyses")
        launch_data = configure_project_run_analyses(
            script_path=script_path,
            output_dir=output_dir,
            languages=languages,
            analyzer_config=analyzers_helper,
            dockerfile_path=dockerfile_path,
            context_dir=pipeline_path,
            image_name=f"project-{dojo_product_name}-builder" if dojo_product_name else "project-builder",
            project_path=project_build_path,
            force_rebuild=False,
            rebuild_images=rebuild_images,
            version=project_version,
            log_level=log_level,
            min_time_class=time_class_level or "",
            analyzers=analyzers,
            pipeline_id=pipeline_id,
        )

        launch_data["languages"] = languages

        with transaction.atomic():
            pipeline = AISTPipeline.objects.select_for_update().get(id=pipeline_id)
            pipeline.launch_data = launch_data or {}
            pipeline.status = AISTStatus.UPLOADING_RESULTS
            pipeline.save(update_fields=["launch_data", "status", "updated"])
        logger.info("Upload step starting")

        repo_path = (launch_data or {}).get("project_path", project_build_path)
        trim_path = (launch_data or {}).get("trim_path", "")

        results = upload_results_internal(
            output_dir=(launch_data or {}).get("output_dir", output_dir),
            analyzers_cfg_path=(launch_data or {}).get("tmp_analyzer_config_path"),
            product_name=dojo_product_name or (project_name or ""),
            repo_path=repo_path,
            trim_path=trim_path,
            pipeline_id=pipeline_id,
            log_level=log_level,
        )

        tests: List[Test] = []
        test_ids = []
        for res in results or []:
            tid = getattr(res, "test_id", None)
            if tid:
                test = Test.objects.filter(id=int(tid)).first()
                test_ids.append(tid)
                if test:
                    tests.append(test)
        # Prepare enrichment or proceed directly to deduplication
        try:
            repo_params = read_repo_params(repo_path)
        except Exception as exc:
            logger.warning("Failed to read repository info from %s: %s", repo_path, exc)
            class _RP:  # minimal fallback
                repo_url = ""
                commit_hash = None
                branch_tag = None
            repo_params = _RP()

        finding_ids: List[int] = list(
            Finding.objects.filter(test_id__in=test_ids).values_list('id', flat=True)
        )

        test_ids = [t.id for t in tests]

        if not finding_ids:
            with transaction.atomic():
                pipeline = AISTPipeline.objects.select_for_update().get(id=pipeline_id)
                pipeline.tests.set(tests, clear=True)
                pipeline.status = AISTStatus.FINISHED
                logger.info("No findings to enrich; Finishing pipeline")
                pipeline.save(update_fields=["status", "updated"])
        else:
            repo_url = getattr(repo_params, "repo_url", "") or ""
            ref = getattr(repo_params, "commit_hash", None) or getattr(repo_params, "branch_tag", None)

            header = [
                enrich_finding_task.s(fid, repo_url, ref, trim_path)
                for fid in finding_ids
            ]
            body = after_upload_enrich_and_watch.s(pipeline_id, test_ids, log_level, params)
            sig = chord(header, body)

            raise self.replace(sig)
    except Ignore:
        raise
    except Exception as exc:
        logger.exception("Exception while running SAST pipeline: %s", exc)
        if pipeline is not None:
            try:
                with transaction.atomic():
                    p = AISTPipeline.objects.select_for_update().get(id=pipeline_id)
                    p.status = AISTStatus.FINISHED
                    p.save(update_fields=["status", "updated"])
            except Exception:
                logger.exception("Failed to mark pipeline as FINISHED after exception.")
        raise


def _csv(items: Iterable[Any]) -> str:
    seen = set()
    result: list[str] = []
    for it in items or []:
        s = str(it).strip()
        if not s:
            continue
        if s not in seen:
            seen.add(s)
            result.append(s)
    return ", ".join(result)

@shared_task(bind=True)
def send_request_to_ai(self, pipeline_id: str, log_level) -> None:
    log = _install_db_logging(pipeline_id, log_level)
    webhook_url = getattr(
        settings,
        "AIST_AI_TRIAGE_WEBHOOK_URL",
        "https://flaming.app.n8n.cloud/webhook-test/triage-sast",
    )

    webhook_timeout = getattr(settings, "AIST_AI_TRIAGE_REQUEST_TIMEOUT", 10)
    triage_secret = getattr(settings, "AIST_AI_TRIAGE_SECRET", None)

    with transaction.atomic():
        try:
            pipeline = (
                AISTPipeline.objects
                .select_for_update()
                .select_related("project__product")
                .get(id=pipeline_id)
            )

            if pipeline.status != AISTStatus.PUSH_TO_AI:
                log.error("Attempt to push to AI before pipeline ready to Push it")
                return

            project = pipeline.project
            product = getattr(project, "product", None)
            project_name = getattr(product, "name", None) or getattr(project, "project_name", "")

            launch_data = pipeline.launch_data or {}
            languages = _csv(launch_data.get("languages") or [])
            tools = _csv(launch_data.get("launched_analyzers") or [])
            callback_url = build_callback_url(pipeline_id)

            payload: Dict[str, Any] = {
                "project": {
                    "name": project_name,
                    "description": getattr(project, "description", "") or "",
                    "languages": languages,
                    "tools": tools,
                },
                "pipeline_id": str(pipeline.id),
                "callback_url": callback_url,
            }
            headers = {"Content-Type": "application/json"}
            body_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            if triage_secret:
                headers["X-AIST-Signature"] = triage_secret
            log.info("Sending AI triage request: url=%s payload=%s", webhook_url, payload)
            resp = requests.post(webhook_url, data=body_bytes, headers=headers, timeout=webhook_timeout)
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.error("AI triage POST failed: %s", exc, exc_info=True)
            pipeline.status = AISTStatus.FINISHED
            pipeline.save(update_fields=["status", "updated"])
            return

        pipeline.status = AISTStatus.WAITING_RESULT_FROM_AI
        pipeline.save(update_fields=["status", "updated"])

        log.info("AI triage request accepted: status=%s body=%s", resp.status_code, resp.text[:500])


@shared_task(bind=True)
def watch_deduplication(self, pipeline_id: str, log_level, params) -> None:
    """Monitor deduplication progress and finalise the pipeline.

    This task polls the ``Test`` model to determine when all imported
    tests have completed deduplication.  Once deduplication is
    finished the pipeline is marked as awaiting AI results and then
    immediately set to finished. Long running polling inside a
    Celery task is acceptable here because we perform a short sleep
    between checks and exit when complete.
    """
    pipeline = AISTPipeline.objects.get(id=pipeline_id)

    logger = _install_db_logging(pipeline_id, log_level)
    if not pipeline.tests:
        pipeline.status = AISTStatus.FINISHED
        pipeline.save(update_fields=['status', 'updated'])
        logger.warning("No tests to wait")
        return
    # Poll until all deduplication flags are true
    try:
        while True:
                # Reload pipeline to capture any manual status change
                pipeline.refresh_from_db()
                # Exit early if user or another task finished the pipeline
                if pipeline.status == AISTStatus.FINISHED:
                    return
                # Query for tests still undergoing deduplication
                remaining = pipeline.tests.filter(deduplication_complete=False).count()
                if remaining > 0:
                    # Sleep for 10 seconds before checking again
                    time.sleep(3)
                    continue
                if params.get("ask_push_to_ai", True):
                    pipeline.status = AISTStatus.WAITING_CONFIRMATION_TO_PUSH_TO_AI
                    pipeline.save(update_fields=["status", "updated"])
                    return
                else:
                    send_request_to_ai.delay(pipeline_id=pipeline_id, log_level=log_level)
    except Exception as exc:
        logger.error("Exception while waiting for deduplication to finish: %s", exc)

GROUP = "aistlog"

CONSUMER = "log-flusher"

def _ensure_group(r):
    """Create consumer group if it does not exist."""
    try:
        r.xgroup_create(STREAM_KEY, GROUP, id="$", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise

@shared_task(bind=True, name="aist.flush_logs_once")
def flush_logs_once(self, max_read: int = 500) -> int:
    """
    Flush a batch of log entries from Redis Stream to the database.

    Reads up to `max_read` entries from the stream, groups them by pipeline_id,
    and appends the concatenated lines to `AISTPipeline.logs`. After writing,
    acknowledges the entries so they won't be re-read.
    Returns the number of log lines persisted on this run.
    """
    r = get_redis()
    # Ensure consumer group exists
    _ensure_group(r)

    # Read entries from the stream for this consumer
    # ">" reads only new messages (not pending)
    response = r.xreadgroup(GROUP, CONSUMER,
                            streams={STREAM_KEY: ">"}, count=max_read, block=200)
    if not response:
        return 0

    # XREADGROUP returns a list of (stream, [(id, fields), ...]) pairs
    entries = response[0][1] if response and response[0][0] == STREAM_KEY else []
    # Group entries by pipeline_id
    by_pid: Dict[str, List[str]] = defaultdict(list)
    entry_ids: List[str] = []

    for entry_id, fields in entries:
        entry_ids.append(entry_id)
        pipeline_id = fields.get("pipeline_id")
        message = fields.get("message", "")
        if not pipeline_id:
            # Skip lines without pipeline_id
            continue
        # prefix with the level or other data if desired
        level = fields.get("level")
        line = f"{level} {message}" if level else message
        by_pid[pipeline_id].append(line)

    # Append each group of lines to the corresponding pipeline.log
    written = 0
    for pid, lines in by_pid.items():
        text_block = "\n".join(lines) + "\n"
        try:
            # Use Concat to update the text atomically without select_for_update
            AISTPipeline.objects.filter(id=pid).update(
                logs=Concat(F("logs"), Value(text_block))
            )
            written += len(lines)
        except Exception:
            # Silently ignore errors; lines will remain pending and can be retried
            continue

    # Acknowledge processed entries
    # If update failed, those entries remain in the pending list and can be retried later
    if entry_ids:
        r.xack(STREAM_KEY, GROUP, *entry_ids)

    return written

class LinkBuilder:
    """Build source links for GitHub/GitLab/Bitbucket; verify remote file existence (handles 429)."""

    """Builds repository links without line anchors, based on repo host and ref."""
    @staticmethod
    def _scm_type(repo_url: str) -> str:
        from urllib.parse import urlparse
        host = urlparse(repo_url).netloc.lower()
        if "github" in host:
            return "github"
        if "gitlab" in host:
            return "gitlab"
        if "bitbucket.org" in host:
            return "bitbucket-cloud"
        if "bitbucket" in host:
            return "bitbucket-server"
        if "gitea" in host:
            return "gitea"
        if "codeberg" in host:
            return "codeberg"
        if "dev.azure.com" in host or "visualstudio.com" in host:
            return "azure"
        return "generic"

    def build(self, repo_url: str, file_path: str, ref: Optional[str]) -> Optional[str]:
        if not repo_url or not file_path:
            return None
        scm = self._scm_type(repo_url)
        ref = ref or "master"
        file_path = file_path.replace("file://","")
        fp = file_path.lstrip("/")
        if scm == "github":
            return f"{repo_url.rstrip('/')}/blob/{ref}/{fp}"
        if scm == "gitlab":
            return f"{repo_url.rstrip('/')}/-/blob/{ref}/{fp}"
        if scm == "bitbucket-cloud":
            return f"{repo_url.rstrip('/')}/src/{ref}/{fp}"
        if scm == "bitbucket-server":
            return f"{repo_url.rstrip('/')}/browse/{fp}?at={ref}"
        if scm in ("gitea", "codeberg"):
            return f"{repo_url.rstrip('/')}/src/{ref}/{fp}"
        if scm == "azure":
            return f"{repo_url.rstrip('/')}/?path=/{fp}&version=GC{ref}"
        return f"{repo_url.rstrip('/')}/blob/{ref}/{fp}"

    @staticmethod
    def remote_link_exists(url: str, timeout: int = 5, max_retries: int = 3) -> Optional[bool]:
        """Return True if GET 200/3xx, False if 404, None for other errors. Retries on 429."""
        try:
            r = requests.get(url, allow_redirects=True, timeout=timeout)
            if r.status_code == 429 and max_retries > 0:
                retry = int(r.headers.get("Retry-After", "1"))
                import time
                time.sleep(retry)
                return LinkBuilder.remote_link_exists(url, timeout, max_retries - 1)
            if r.status_code == 200:
                return True
            if 300 <= r.status_code < 400:
                return True
            if r.status_code == 404:
                return False
            return None
        except requests.RequestException:
            return None

@shared_task(bind=False)
def enrich_finding_task(
    finding_id: int,
    repo_url: str,
    ref: Optional[str],
    trim_path: str,
) -> int:
    """Enrich a single finding by trimming its file path and attaching a source link.

    This task mirrors the logic contained in the internal importer for
    processing a single finding. It loads the finding from the
    database, optionally trims the file path, constructs a remote link
    via :class:`LinkBuilder`, tests whether the link exists and either
    attaches metadata or deletes the finding accordingly.

    :param finding_id: The ID of the finding to process.
    :param repo_url: The base repository URL to use for link construction.
    :param ref: A branch or commit hash used to qualify the link.
    :param trim_path: A prefix to remove from the finding's file_path.
    :returns: 1 when the finding was enriched, 0 otherwise.
    """
    try:
        f = Finding.objects.select_related("test__engagement").get(id=finding_id)
    except Finding.DoesNotExist:
        return 0
    try:
        file_path = f.file_path or ""
        # Trim the path
        if trim_path and file_path.startswith(trim_path):
            tp = trim_path if trim_path.endswith("/") else trim_path + "/"
            f.file_path = file_path.replace(tp, "")
            f.save(update_fields=["file_path"])
            file_path = f.file_path
        # Build a link
        linker = LinkBuilder()
        link = linker.build(repo_url or "", file_path, ref)
        if not link:
            return 0
        exists = linker.remote_link_exists(link)
        if exists is True:
            # Attach or update metadata
            DojoMeta.objects.update_or_create(
                finding=f,
                name="sourcefile_link",
                value=link,
            )
            return 1
        if exists is False:
            f.delete()
            return 0
        # Unknown status: skip enrichment
        return 0
    except Exception as e:
        return 0

@shared_task(name="aist.after_upload_enrich_and_watch")
def after_upload_enrich_and_watch(results: list[int],
                                  pipeline_id: str,
                                  test_ids: list[int],
                                  log_level,
                                  params) -> None:
    logger = _install_db_logging(pipeline_id, log_level)
    enriched = sum(int(v or 0) for v in results)

    with transaction.atomic():
        pipeline = AISTPipeline.objects.select_for_update().get(id=pipeline_id)

        if test_ids:
            tests = list(Test.objects.filter(id__in=test_ids))
            pipeline.tests.set(tests, clear=True)

        pipeline.status = AISTStatus.WAITING_DEDUPLICATION_TO_FINISH
        pipeline.save(update_fields=["status", "updated"])

    logger.info("Enrichment finished: %s findings enriched. Waiting for deduplication.", enriched)
    res = watch_deduplication.delay(pipeline_id=pipeline_id, log_level=log_level, params=params)

    with transaction.atomic():
        pipeline.watch_dedup_task_id = res.id
        pipeline.save(update_fields=["watch_dedup_task_id", "updated"])



@shared_task(
    name="dojo.aist.reconcile_deduplication",
    rate_limit="5/s",                    # throttle to protect the DB
    autoretry_for=(OperationalError,),   # retry on transient DB issues
    retry_backoff=True,
    retry_jitter=True,
    max_retries=5,
)
def reconcile_deduplication(batch_size: int = 200, max_runtime_s: int = 50) -> dict:
    """
    Periodically reconciles deduplication state for Tests whose deduplication is not complete.
    To handle concurrent runs safely, we take a row-level lock on the progress row and skip
    already-locked rows (skip_locked=True).
    """
    start = time.time()
    processed = 0

    # Single read to fetch candidate Tests (with the FK preloaded for convenience).
    tests = (
        Test.objects
        .filter(deduplication_complete=False)
        .select_related("dedupe_progress")
        .order_by("updated" if hasattr(Test, "updated") else "id")[:batch_size]
    )

    for test in tests:
        if time.time() - start > max_runtime_s:
            break

        try:
            prog = test.dedupe_progress
        except TestDeduplicationProgress.DoesNotExist:
            prog = None

        if prog is None:
            continue  # no progress row yet — a later run can create/handle it

        # Strict per-item reconciliation with row lock on the progress record.
        with transaction.atomic():
            # Lock just this progress row; if another worker already locked it,
            # skip it now to avoid contention and try others.
            locked_prog = (
                TestDeduplicationProgress.objects
                .select_for_update(skip_locked=True)
                .get(pk=prog.pk)
            )

            # Idempotent operation: derives state from DB, safe to run multiple times.
            locked_prog.refresh_pending_tasks()
            processed += 1
    return {
        "processed": processed,
        "batch_size": batch_size,
        "duration_s": round(time.time() - start, 3),
    }