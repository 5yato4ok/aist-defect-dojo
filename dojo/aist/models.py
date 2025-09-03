from __future__ import annotations
from django.db import models
from django.utils import timezone
from dojo.models import Test

class AISTStatus(models.TextChoices):
    SAST_LAUNCHED = "SAST_LAUNCHED", "Launched"
    UPLOADING_RESULTS = "UPLOADING_RESULTS", "Uploading Results"
    WAITING_DEDUPLICATION_FINISHED = "WAITING_DEDUPLICATION_FINISHED", "Waiting Deduplication Finished"
    WAITING_RESULT_FROM_AI = "WAITING_RESULT_FROM_AI", "Waiting Result From AI"
    FINISHED = "FINISHED", "Finished"

class AISTProject(models.Model):
    created = models.DateTimeField(default=timezone.now, editable=False)
    updated = models.DateTimeField(auto_now=True)

    project_name = models.CharField(max_length=255, unique=True)
    supported_languages = models.JSONField(default=list, blank=True)
    script_path = models.CharField(max_length=1024)
    project_path = models.CharField(max_length=1024)
    project_version = models.CharField(max_length=255, null=True, blank=True)
    output_dir = models.CharField(max_length=1024, default="/tmp/aist-output")

    def __str__(self) -> str:
        return self.project_name


class AISTPipeline(models.Model):
    created = models.DateTimeField(default=timezone.now, editable=False)
    updated = models.DateTimeField(auto_now=True)
    started = models.DateTimeField(auto_now=True)

    id = models.CharField(primary_key=True, max_length=64)

    project = models.ForeignKey(AISTProject, on_delete=models.CASCADE, related_name="aist_pipelines")
    status = models.CharField(max_length=64, choices=AISTStatus.choices, default=AISTStatus.SAST_LAUNCHED)

    tests = models.ManyToManyField(Test, related_name="aist_pipelines", blank=True)
    launch_data = models.JSONField(default=dict, blank=True)
    logs = models.TextField(default="", blank=True)

    # Optional: Celery task ids to enable STOP action
    run_task_id = models.CharField(max_length=64, null=True, blank=True)
    watch_dedup_task_id = models.CharField(max_length=64, null=True, blank=True)

    class Meta:
        ordering = ("-created",)

    def append_log(self, line: str) -> None:
        txt = self.logs or ""
        if not line.endswith("\n"):
            line += "\n"
        self.logs = txt + line
        self.save(update_fields=["logs", "updated"])

    def __str__(self) -> str:
        return f"SASTPipeline[{self.id}] {self.status}"
