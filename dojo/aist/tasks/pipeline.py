import os
from django.conf import settings
from django.utils import timezone
from dojo.aist.utils import _import_sast_pipeline_package
from typing import Any, Dict, List, Optional
from celery import shared_task, chord, chain
from django.db import transaction, OperationalError
from dojo.models import Test, Finding
from dojo.aist.models import AISTPipeline, AISTStatus

from dojo.aist.logging_transport import get_redis, _install_db_logging
from .enrich import make_enrich_chord
from dojo.aist.pipeline_args import PipelineArguments

_import_sast_pipeline_package()

from pipeline.project_builder import configure_project_run_analyses  # type: ignore
from pipeline.config_utils import AnalyzersConfigHelper  # type: ignore
from pipeline.defect_dojo.repo_info import read_repo_params  # type: ignore
from pipeline.docker_utils import cleanup_pipeline_containers # type: ignore
from dojo.aist.internal_upload import upload_results_internal
from celery.exceptions import Ignore


@shared_task(bind=True)
def run_sast_pipeline(self, pipeline_id: str, params: dict) -> None:
    """
    Execute a SAST pipeline asynchronously.

    This task coordinates the SAST pipeline by invoking the configure
    and upload functions provided by the external ``sast-pipeline``
    package. All progress is recorded in the database so that
    connected clients can observe status changes and log output in real time.

    :param pipeline_id: Primary key of the :class:`AISTPipeline` instance.
    :param params: Dictionary of parameters collected from the form.
    """

    log_level = params.get("log_level", "INFO")
    logger = _install_db_logging(pipeline_id, log_level)

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
            if params is None:
                logger.info("Launch via API. Using default parameters for project.")
                params = PipelineArguments(project=pipeline.project, project_version = '')
            else:
                params = PipelineArguments.from_dict(params)

        analyzers_helper = AnalyzersConfigHelper()
        project_name = params.project_name
        languages = params.languages
        project_version = params.project_version
        output_dir =  params.output_dir
        rebuild_images = params.rebuild_images
        analyzers = params.analyzers
        time_class_level = params.time_class_level # FIXME: if analyzer is from slower time class it will be skipped
        script_path = params.script_path
        dockerfile_path = params.dockerfile_path

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
            context_dir=params.pipeline_src_path,
            image_name=f"project-{project_name}-builder" if project_name else "project-builder",
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
            pipeline.launch_data = launch_data
            pipeline.status = AISTStatus.UPLOADING_RESULTS
            pipeline.save(update_fields=["launch_data", "status", "updated"])
        logger.info("Upload step starting")

        repo_path = launch_data.get("project_path", project_build_path)
        trim_path = launch_data.get("trim_path", "")

        results = upload_results_internal(
            output_dir=launch_data.get("output_dir", output_dir),
            analyzers_cfg_path=launch_data.get("tmp_analyzer_config_path"),
            product_name=project_name,
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
            logger.error("Failed to read repository info from %s: %s", repo_path, exc)
            return

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
            with transaction.atomic():
                pipeline = AISTPipeline.objects.select_for_update().get(id=pipeline_id)
                pipeline.status = AISTStatus.FINDING_POSTPROCESSING
                pipeline.save(update_fields=["status", "updated"])

            repo_url = getattr(repo_params, "repo_url", "") or ""
            ref = getattr(repo_params, "commit_hash", None) or getattr(repo_params, "branch_tag", None)
            redis = get_redis()
            redis.hset(f"aist:progress:{pipeline_id}:enrich", mapping={"total": len(finding_ids), "done": 0})
            sig = make_enrich_chord(
                finding_ids=finding_ids,
                repo_url=repo_url,
                ref=ref,
                trim_path=trim_path,
                pipeline_id=pipeline_id,
                test_ids=test_ids,
                log_level=log_level
            )
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