from __future__ import annotations
from django.db import models
from django.utils import timezone
from sqlalchemy.testing.suite import AutocommitIsolationTest

from dojo.models import Test
from unittests.test_adminsite import AdminSite


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

    # pipeline id equals PIPELINE_ID from sast-pipeline
    id = models.CharField(primary_key=True, max_length=64)

    project = models.ForeignKey('AISTProject', on_delete=models.CASCADE, related_name='aist_pipelines')
    status = models.CharField(max_length=64, choices=AISTStatus.choices, default=AISTStatus.SAST_LAUNCHED)

    tests = models.ManyToManyField(Test, related_name="aist_pipelines", blank=True)

    # JSON blob like launch_description.json
    launch_data = models.JSONField(default=dict, blank=True)

    # running logs
    logs = models.TextField(default="", blank=True)

    class Meta:
        ordering = ("-created",)

    def append_log(self, line: str) -> None:
        self.logs = (self.logs or "") + (line if line.endswith("\n") else line + "\n")
        self.save(update_fields=["logs", "updated"])

    def __str__(self) -> str:
        return f"SASTPipeline[{self.id}] {self.status}"
