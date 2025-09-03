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
from typing import Any, Dict, List

from celery import shared_task, current_app
from django.conf import settings
from dojo.models import Test
from django.db import transaction
from django.apps import apps

from .models import AISTPipeline, AISTStatus
from .utils import DatabaseLogHandler


@shared_task(bind=True)
def run_sast_pipeline(self, pipeline_id: str, params: Dict[str, Any]) -> None:
    """Execute a SAST pipeline asynchronously.

    This task coordinates the SAST pipeline by invoking the configure
    and upload functions provided by the external ``sast-pipeline``
    package.  All progress is recorded in the database so that
    connected clients can observe status changes and log output in
    real time.

    :param pipeline_pk: Primary key of the :class:`SASTPipeline`
        instance.
    :param params: Dictionary of parameters collected from the form.
    """
    # Retrieve the pipeline instance up front.  Using select_for_update
    # prevents concurrent updates to the same row while this task is
    # running.
    pipeline = AISTPipeline.objects.select_for_update().get(id=pipeline_id)
    logger = logging.getLogger(f"dojo.sast.pipeline.{pipeline_id}")
    logger.setLevel(logging.INFO)
    handler = DatabaseLogHandler(pipeline_id)
    # Ensure we propagate messages to the root so they can be captured
    logger.addHandler(handler)
    try:
        # Dynamically load the SAST pipeline code.  Append the code path
        # to sys.path so that Python can resolve imports.  We avoid
        # importing these modules at the top level to allow running
        # without the code present (e.g. during unit testing).
        pipeline_path = getattr(settings, 'AIST_PIPELINE_CODE_PATH', None)
        if not pipeline_path or not os.path.isdir(pipeline_path):
            raise RuntimeError('SAST pipeline code path is not configured or does not exist. Please set AIST_PIPELINE_CODE_PATH.')
        if pipeline_path not in sys.path:
            sys.path.append(pipeline_path)
        # Import after modifying sys.path
        from pipeline.project_builder import configure_project_run_analyses  # type: ignore
        from pipeline.defect_dojo.utils import upload_results  # type: ignore
        from pipeline.config_utils import AnalyzersConfigHelper  # type: ignore

        os.environ["PIPELINE_ID"] = pipeline_id

        # Build the analyzer configuration helper
        analyzers_cfg_path = os.path.join(pipeline_path, 'pipeline', 'config', 'analyzers.yaml')
        analyzers_helper = AnalyzersConfigHelper(analyzers_cfg_path)

        # Determine run parameters using either the form values or the
        # defaults stored on the associated project.  Fall back to
        # sensible defaults when not provided.
        def get_param(key: str, default: Any) -> Any:
            return params.get(key) if params.get(key) not in (None, '', []) else default

        project = pipeline.project
        script_path = get_param('script_path', None)
        output_dir = get_param('output_dir', os.path.join('/tmp', 'aist_output', project.project_name))
        languages = get_param('languages', project.supported_languages if project else [])
        # Flatten languages to a list
        if isinstance(languages, str):
            languages = [languages]
        project_version = get_param('project_version', project.project_version if project else None)
        rebuild_images = bool(get_param('rebuild_images', False))
        analyzers = get_param('analyzers', [])
        time_class_level = get_param('time_class_level', '')
        log_level = get_param('log_level', 'INFO')
        dojo_product_name = get_param('dojo_product_name', project.project_name if project else None)

        # Determine additional paths required by the builder.  The Docker
        # context and Dockerfile live within the SAST pipeline code
        # directory.  Ensure these paths are absolute so that docker
        # correctly binds them.
        dockerfile_path = os.path.join(pipeline_path, 'pipeline', 'project-builder', 'Dockerfile')
        context_dir = os.path.join(pipeline_path, 'pipeline')
        project_path = get_param('project_path', project.project_path if project else None)

        # Launch the builder and analyzers
        logger.info('Starting configure_project_run_analyses')
        launch_data = configure_project_run_analyses(
            script_path=script_path,
            output_dir=output_dir,
            languages=languages,
            analyzer_config=analyzers_helper,
            dockerfile_path=dockerfile_path,
            context_dir=context_dir,
            image_name=f"project-{dojo_product_name}-builder",
            project_path=project_path,
            force_rebuild=False,
            rebuild_images=rebuild_images,
            version=project_version,
            log_level=log_level,
            min_time_class=time_class_level or '',
            analyzers=analyzers,
        )
        # Persist launch metadata for later inspection
        pipeline.launch_data = launch_data or {}
        pipeline.status = AISTStatus.UPLOADING_RESULTS
        pipeline.save(update_fields=['launch_data', 'status', 'updated'])
        logger.info('Upload step starting')

        # Prepare arguments for the result upload.  The SAST pipeline code
        # expects a DefectDojo configuration file.  We reuse the
        # default file provided by the pipeline.  You may override
        # this via an environment variable if required.
        dojo_cfg_path = os.path.join(pipeline_path, 'pipeline', 'config', 'defectdojo.yaml')
        # Use the trimmed path returned by the build or fall back to project path
        repo_path = launch_data.get('project_path', project_path) if launch_data else project_path
        trim_path = launch_data.get('trim_path') if launch_data else ''

        # Perform the upload
        results = upload_results(
            output_dir=launch_data.get('output_dir', output_dir),
            analyzers_cfg_path=launch_data.get('tmp_analyzer_config_path'),
            product_name=dojo_product_name or (project.project_name if project else ''),
            dojo_config_path=dojo_cfg_path,
            repo_path=repo_path,
            trim_path=trim_path,
        )
        # Extract test IDs from the returned ImportResult objects
        tests: List[Test] = []
        for res in results:
            try:
                tid = getattr(res, 'test_id', None)
                if tid:
                    test = Test.objects.filter(id=int(tid)).first()
                    if test:
                        tests.append(test)
            except Exception:
                continue

        pipeline.tests.set(tests)
        pipeline.status = AISTStatus.WAITING_DEDUPLICATION_FINISHED
        pipeline.save(update_fields=['tests', 'status', 'updated'])
        logger.info('Results uploaded; waiting for deduplication')
        # Schedule a follow up task to monitor deduplication.  Use a
        # countdown to delay the first check rather than blocking
        # synchronously.
        watch_deduplication.apply_async(args=[pipeline_id], countdown=60)
    except Exception as exc:
        # Log the exception and mark the pipeline as finished.  The
        # exception is re‑raised so Celery can record it on the task
        # result backend if configured.
        logger.exception('Exception while running SAST pipeline: %s', exc)
        pipeline.status = AISTStatus.FINISHED
        pipeline.save(update_fields=['status', 'updated'])
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
    Test = apps.get_model('dojo', 'Test')
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