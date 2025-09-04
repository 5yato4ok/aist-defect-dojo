"""
Celery tasks for the SAST pipeline integration.

The tasks defined in this module orchestrate the long running
operations involved in running the SAST pipeline.  These tasks run
asynchronously within Celery workers so that the main web process
remains responsive.  Logs are streamed back to the database via the
``DatabaseLogHandler``.

``run_sast_pipeline`` performs the following steps:

* Generates a unique pipeline identifier by invoking
  ``docker_utils.get_pipeline_id``.
* Calls ``configure_project_run_analyses`` to build the project and run
  analyzers.  The returned launch data is persisted on the pipeline.
* Uploads results to DefectDojo via ``upload_results`` and records the
  resulting test IDs.
* Transitions the pipeline status through each phase and schedules
  monitoring of deduplication progress.

``watch_deduplication`` polls the database periodically to detect
when deduplication has completed for all tests imported by a
pipeline.  Once complete the pipeline is marked as awaiting AI
enrichment, then immediately marked finished (the AI step is
reserved for future enhancement).
"""

from __future__ import annotations

import os
import json
import requests
import sys
import time
import logging
from django.utils import timezone
from typing import Any, Dict, List

from celery import shared_task, current_app
from django.conf import settings
from dojo.models import Test
from django.db import transaction
from django.apps import apps
from django.urls import reverse

from .models import AISTPipeline, AISTStatus
from .utils import DatabaseLogHandler, build_callback_url
from ..jira_link.helper import process_jira_project_form
from typing import Any, Dict, Iterable


def _install_db_logging(pipeline_id: str, level=logging.INFO):
    """Connect BD -handler for all required loggers ко всем нужным логгерам (root и 'pipeline')."""
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    dbh = DatabaseLogHandler(pipeline_id)
    dbh.setLevel(level)
    dbh.setFormatter(fmt)

    plog = logging.getLogger("pipeline")
    plog.setLevel(level)
    plog.propagate = True
    if not any(isinstance(h, DatabaseLogHandler) and getattr(h, "pipeline_id", None) == pipeline_id
               for h in plog.handlers):
        plog.addHandler(dbh)

    root = logging.getLogger("dojo.aist.pipeline.{pipeline_id}")
    root.setLevel(level)
    if not any(isinstance(h, DatabaseLogHandler) and getattr(h, "pipeline_id", None) == pipeline_id
               for h in root.handlers):
        root.addHandler(dbh)
    return root

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
            if pipeline.status in {AISTStatus.SAST_LAUNCHED, AISTStatus.UPLOADING_RESULTS, AISTStatus.WAITING_DEDUPLICATION_TO_FINISH, AISTStatus.WAITING_RESULT_FROM_AI}:
                logger.info("Pipeline already in progress; skipping duplicate start.")
                return

            pipeline.status = AISTStatus.SAST_LAUNCHED
            pipeline.started = timezone.now()
            pipeline.save(update_fields=["status", "started", "updated"])


            project = pipeline.project
            project_name = project.product.name if project else None
            project_version = project.project_version if project else None
            project_supported_languages = project.supported_languages if project else []


        pipeline_path = getattr(settings, "AIST_PIPELINE_CODE_PATH", None)
        if not pipeline_path or not os.path.isdir(pipeline_path):
            raise RuntimeError(
                "SAST pipeline code path is not configured or does not exist. "
                "Please set AIST_PIPELINE_CODE_PATH."
            )
        if pipeline_path not in sys.path:
            sys.path.append(pipeline_path)


        from pipeline.project_builder import configure_project_run_analyses  # type: ignore
        from pipeline.defect_dojo.utils import upload_results               # type: ignore
        from pipeline.config_utils import AnalyzersConfigHelper             # type: ignore

        os.environ["PIPELINE_ID"] = str(pipeline_id)

        analyzers_helper = AnalyzersConfigHelper()

        script_path = get_param("script_path", None)
        if not os.path.isfile(script_path):
            raise RuntimeError("Incorrrect script path for AIST pipeline.")

        output_dir = get_param("output_dir", os.path.join("/tmp", "aist_output", project_name or "project"))
        languages = get_param("languages", [])
        if isinstance(languages, str):
            languages = [languages]
        languages = list(set(languages + project_supported_languages))
        project_version = get_param("project_version", project_version)
        rebuild_images = bool(get_param("rebuild_images", False))
        analyzers = get_param("analyzers", [])
        time_class_level = get_param("time_class_level", "")
        dojo_product_name = get_param("dojo_product_name", project_name or None)

        dockerfile_path = os.path.join(pipeline_path, "Dockerfiles", "builder", "Dockerfile")
        if not os.path.isfile(dockerfile_path):
            raise RuntimeError("Dockerfile does not exist")

        results_path = getattr(settings, "AIST_RESULTS_DIR", None)
        if not results_path or not os.path.isdir(results_path):
            raise RuntimeError("Results path for AIST is not setup")

        logger.info("Starting configure_project_run_analyses")
        launch_data = configure_project_run_analyses(
            script_path=script_path,
            output_dir=output_dir,
            languages=languages,
            analyzer_config=analyzers_helper,
            dockerfile_path=dockerfile_path,
            context_dir=pipeline_path,
            image_name=f"project-{dojo_product_name}-builder" if dojo_product_name else "project-builder",
            project_path=results_path,
            force_rebuild=False,
            rebuild_images=rebuild_images,
            version=project_version,
            log_level=log_level,
            min_time_class=time_class_level or "",
            analyzers=analyzers,
        )

        launch_data["languages"] = languages

        with transaction.atomic():
            pipeline = AISTPipeline.objects.select_for_update().get(id=pipeline_id)
            pipeline.launch_data = launch_data or {}
            pipeline.status = AISTStatus.UPLOADING_RESULTS
            pipeline.save(update_fields=["launch_data", "status", "updated"])
        logger.info("Upload step starting")

        dojo_cfg_path = os.path.join(pipeline_path, "pipeline", "config", "defectdojo.yaml")
        repo_path = (launch_data or {}).get("project_path", results_path)
        trim_path = (launch_data or {}).get("trim_path", "")

        results = upload_results( #TODO: change to usage defect dojo classes
            output_dir=(launch_data or {}).get("output_dir", output_dir),
            analyzers_cfg_path=(launch_data or {}).get("tmp_analyzer_config_path"),
            product_name=dojo_product_name or (project_name or ""),
            dojo_config_path=dojo_cfg_path,
            repo_path=repo_path,
            trim_path=trim_path,
        )

        tests: List[Test] = []
        for res in results or []:
            tid = getattr(res, "test_id", None)
            if tid:
                test = Test.objects.filter(id=int(tid)).first()
                if test:
                    tests.append(test)

        with transaction.atomic():
            pipeline = AISTPipeline.objects.select_for_update().get(id=pipeline_id)
            pipeline.tests.set(tests, clear=True)
            pipeline.status = AISTStatus.WAITING_DEDUPLICATION_TO_FINISH
            pipeline.save(update_fields=["status", "updated"])
            logger.info("Results uploaded; waiting for deduplication")

            res = watch_deduplication.apply_async(args=[pipeline_id, log_level, params], countdown=5)
            pipeline.watch_dedup_task_id = res.id
            pipeline.save(update_fields=["watch_dedup_task_id", "updated"])

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

def send_request_to_ai(pipeline_id: str, log_level) -> None:
    log = _install_db_logging(pipeline_id, log_level)
    webhook_url = getattr(
        settings,
        "AIST_AI_TRIAGE_WEBHOOK_URL",
        "https://flaming.app.n8n.cloud/webhook-test/triage-sast",
    )

    webhook_timeout = getattr(settings, "AIST_AI_TRIAGE_REQUEST_TIMEOUT", 10)
    triage_secret = getattr(settings, "AIST_AI_TRIAGE_SECRET", None)

    with transaction.atomic():
        pipeline = (
            AISTPipeline.objects
            .select_for_update()
            .select_related("project__product")
            .get(id=pipeline_id)
        )

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
        try:
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
                    time.sleep(10)
                    continue

                send_request_to_ai(pipeline_id,log_level)
                return
    except Exception as exc:
        logger.error("Exception while waiting for deduplication to finish: %s", exc)