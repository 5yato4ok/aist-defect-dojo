from __future__ import annotations
from django.db import models
from django.utils import timezone
from dojo.models import Test, Product

class AISTStatus(models.TextChoices):
    SAST_LAUNCHED = "SAST_LAUNCHED", "Launched"
    UPLOADING_RESULTS = "UPLOADING_RESULTS", "Uploading Results"
    WAITING_DEDUPLICATION_TO_FINISH = "WAITING_DEDUPLICATION_TO_FINISH", "Waiting Deduplication To Finish"
    WAITING_CONFIRMATION_TO_PUSH_TO_AI = "WAITING_CONFIRMATION_TO_PUSH_TO_AI", "Waiting Confirmation To Push to AI"
    PUSH_TO_AI = "PUSH_TO_AI", "Push to AI"
    WAITING_RESULT_FROM_AI = "WAITING_RESULT_FROM_AI", "Waiting Result From AI"
    FINISHED = "FINISHED", "Finished"

class AISTProject(models.Model):
    created = models.DateTimeField(default=timezone.now, editable=False)
    updated = models.DateTimeField(auto_now=True)

    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    supported_languages = models.JSONField(default=list, blank=True)
    script_path = models.CharField(max_length=1024)
    output_dir = models.CharField(max_length=1024, default="/tmp/aist/output")
    compilable = models.BooleanField(default=False)

    def __str__(self) -> str:
        return self.product.name

class AISTProjectVersion(models.Model):
    project = models.ForeignKey(
        AISTProject, on_delete=models.CASCADE, related_name="versions"
    )
    version = models.CharField(max_length=64, db_index=True)
    description = models.TextField(blank=True)
    metadata = models.JSONField(default=dict, blank=True)

    created = models.DateTimeField(auto_now_add=True, editable=False)
    updated = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["project", "version"],
                name="uniq_project_version_per_project",
            )
        ]
        ordering = ["-created"]

    def __str__(self):
        return f"{self.project_id}:{self.version}"


class AISTPipeline(models.Model):
    created = models.DateTimeField(default=timezone.now, editable=False)
    updated = models.DateTimeField(auto_now=True)
    started = models.DateTimeField(auto_now=True)

    id = models.CharField(primary_key=True, max_length=64)

    project = models.ForeignKey(AISTProject, on_delete=models.PROTECT, related_name="aist_pipelines")
    project_version = models.ForeignKey(
        AISTProjectVersion,
        on_delete=models.PROTECT,
        related_name="pipelines",
        db_index=True,
        null=True, blank=True,
    )
    status = models.CharField(max_length=64, choices=AISTStatus.choices, default=AISTStatus.FINISHED)

    tests = models.ManyToManyField(Test, related_name="aist_pipelines", blank=True)
    launch_data = models.JSONField(default=dict, blank=True)
    logs = models.TextField(default="", blank=True)

    run_task_id = models.CharField(max_length=64, null=True, blank=True)
    watch_dedup_task_id = models.CharField(max_length=64, null=True, blank=True)

    response_from_ai = models.JSONField(default=dict, blank=True)

    class Meta:
        ordering = ("-created",)

    def __str__(self) -> str:
        return f"SASTPipeline[{self.id}] {self.status}"

    def append_log(self, line: str) -> None:
        txt = self.logs or ""
        if not line.endswith("\n"):
            line += "\n"
        self.logs = txt + line
        self.save(update_fields=["logs", "updated"])
