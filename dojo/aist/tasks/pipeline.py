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

_import_sast_pipeline_package()

from pipeline.project_builder import configure_project_run_analyses  # type: ignore
from pipeline.config_utils import AnalyzersConfigHelper  # type: ignore
from pipeline.defect_dojo.repo_info import read_repo_params  # type: ignore
from pipeline.docker_utils import cleanup_pipeline_containers # type: ignore
from dojo.aist.internal_upload import upload_results_internal
from celery.exceptions import Ignore

def fill_default_project_parameters():
    # project version
    # project_supported_languages = project.supported_languages
    # rebuild_images = False
    # analyzers = default list of analyzers
    # time_class_level = ""
    # project_name
    # script path
    #
    pass

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

    if params is None:
        logger.info("Launch via API. Need to add default parameters for project")
        return

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
            project_version = pipeline.project_version.version if pipeline.project_version else None # ???
            project_supported_languages = project.supported_languages if project else []

        analyzers_helper = AnalyzersConfigHelper()
        languages = get_param("languages", [])
        if isinstance(languages, str):
            languages = [languages]
        languages = list(set(languages + project_supported_languages))
        project_version = get_param("project_version", project_version)
        aist_path = getattr(settings, "AIST_OUTPUT_PATH", os.path.join("/tmp", "aist", "output"))
        output_dir =  os.path.join(aist_path, project_name or "project", project_version or "default")
        rebuild_images = bool(get_param("rebuild_images", False))
        analyzers = get_param("analyzers", [])
        time_class_level = get_param("time_class_level", "") # FIXME: if analyzer is from slower time class it will be skipped
        dojo_product_name = get_param("dojo_product_name", project_name or None) # ???

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
                log_level=log_level,
                params=params,
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