import requests
from dojo.models import Test, Finding, DojoMeta
from celery import shared_task, current_app, chord, chain
from dojo.aist.logging_transport import get_redis
from typing import Any, Dict, List, Optional
from dojo.aist.logging_transport import get_redis, _install_db_logging
from django.db import transaction
from dojo.aist.models import AISTPipeline, AISTStatus
from .dedup import watch_deduplication


class LinkBuilder:
    """Build source links for GitHub/GitLab/Bitbucket; verify remote file existence (handles 429)."""
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

@shared_task(bind=True)
def report_enrich_done(self, result: int, pipeline_id: str):
    redis = get_redis()
    key = f"aist:progress:{pipeline_id}:enrich"
    redis.hincrby(key, "done", 1)
    return result

@shared_task(name="dojo.aist.after_upload_enrich_and_watch")
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