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

from .models import AISTPipeline, AISTStatus
from .utils import DatabaseLogHandler

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
    root.setLevel(min(root.level or level, level))
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
            if pipeline.status in {AISTStatus.SAST_LAUNCHED, AISTStatus.UPLOADING_RESULTS, AISTStatus.WAITING_DEDUPLICATION_FINISHED, AISTStatus.WAITING_RESULT_FROM_AI}:
                logger.info("Pipeline already in progress; skipping duplicate start.")
                return

            pipeline.status = AISTStatus.SAST_LAUNCHED
            pipeline.started = timezone.now()
            pipeline.save(update_fields=["status", "started", "updated"])


            project = pipeline.project
            project_name = project.project_name if project else None
            project_version = project.project_version if project else None
            project_path_default = project.project_path if project else None
            supported_languages = project.supported_languages if project else []


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

        analyzers_cfg_path = os.path.join(pipeline_path, "pipeline", "config", "analyzers.yaml")
        analyzers_helper = AnalyzersConfigHelper(analyzers_cfg_path)

        script_path = get_param("script_path", None)
        output_dir = get_param("output_dir", os.path.join("/tmp", "aist_output", project_name or "project"))
        languages = get_param("languages", supported_languages)
        if isinstance(languages, str):
            languages = [languages]
        project_version = get_param("project_version", project_version)
        rebuild_images = bool(get_param("rebuild_images", False))
        analyzers = get_param("analyzers", [])
        time_class_level = get_param("time_class_level", "")
        dojo_product_name = get_param("dojo_product_name", project_name or None)

        dockerfile_path = os.path.join(pipeline_path, "Dockerfiles", "builder", "Dockerfile")
        project_path = get_param("project_path", project_path_default)

        logger.info("Starting configure_project_run_analyses")
        launch_data = configure_project_run_analyses(
            script_path=script_path,
            output_dir=output_dir,
            languages=languages,
            analyzer_config=analyzers_helper,
            dockerfile_path=dockerfile_path,
            context_dir=pipeline_path,
            image_name=f"project-{dojo_product_name}-builder" if dojo_product_name else "project-builder",
            project_path=project_path,
            force_rebuild=False,
            rebuild_images=rebuild_images,
            version=project_version,
            log_level=log_level,
            min_time_class=time_class_level or "",
            analyzers=analyzers,
        )

        with transaction.atomic():
            # пере-«пришиваем» объект, чтобы избежать stale instance
            pipeline = AISTPipeline.objects.select_for_update().get(id=pipeline_id)
            pipeline.launch_data = launch_data or {}
            pipeline.status = AISTStatus.UPLOADING_RESULTS
            pipeline.save(update_fields=["launch_data", "status", "updated"])
        logger.info("Upload step starting")

        dojo_cfg_path = os.path.join(pipeline_path, "pipeline", "config", "defectdojo.yaml")
        repo_path = (launch_data or {}).get("project_path", project_path)
        trim_path = (launch_data or {}).get("trim_path", "")

        results = upload_results(
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
            pipeline.status = AISTStatus.WAITING_DEDUPLICATION_FINISHED
            pipeline.save(update_fields=["status", "updated"])
            logger.info("Results uploaded; waiting for deduplication")

            res = watch_deduplication.apply_async(args=[pipeline_id], countdown=60)
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
    finally:
        logger.removeHandler(handler)


@shared_task(bind=True)
def watch_deduplication(self, pipeline_id: int) -> None:
    """Monitor deduplication progress and finalise the pipeline.

    This task polls the ``Test`` model to determine when all imported
    tests have completed deduplication.  Once deduplication is
    finished the pipeline is marked as awaiting AI results and then
    immediately set to finished.  Long running polling inside a
    Celery task is acceptable here because we perform a short sleep
    between checks and exit when complete.
    """
    # Resolve Test model lazily to avoid circular imports
    pipeline = AISTPipeline.objects.get(id=pipeline_id)
    # If there are no test IDs recorded something went wrong; finish early
    if not pipeline.tests:
        pipeline.status = AISTStatus.FINISHED
        pipeline.save(update_fields=['status', 'updated'])
        return
    # Poll until all deduplication flags are true
    while True:
        # Reload pipeline to capture any manual status change
        pipeline.refresh_from_db()
        # Exit early if user or another task finished the pipeline
        if pipeline.status == AISTStatus.FINISHED:
            return
        # Query for tests still undergoing deduplication
        remaining = pipeline.tests.filter(deduplication_finished=False).count()
        if remaining > 0:
            # Sleep for a minute before checking again
            time.sleep(60)
            continue
        # All tests deduplicated
        pipeline.status = AISTStatus.WAITING_RESULT_FROM_AI
        pipeline.save(update_fields=['status', 'updated'])
        # Placeholder for future AI integration.  In this initial
        # version we immediately mark the pipeline finished.
        pipeline.status = AISTStatus.FINISHED
        pipeline.save(update_fields=['status', 'updated'])
        return