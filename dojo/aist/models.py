from __future__ import annotations
from django.db import models
from django.utils import timezone
from dojo.models import Test, Product, Finding

class AISTStatus(models.TextChoices):
    SAST_LAUNCHED = "SAST_LAUNCHED", "Launched"
    UPLOADING_RESULTS = "UPLOADING_RESULTS", "Uploading Results"
    FINDING_POSTPROCESSING = "FINDING_POSTPROCESSING", "Finding post-processing"
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

class TestDeduplicationProgress(models.Model):
    """Deduplication progress on one Test."""
    test = models.OneToOneField(
        Test, on_delete=models.CASCADE, related_name="dedupe_progress"
    )
    pending_tasks = models.PositiveIntegerField(default=0)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    deduplication_complete = models.BooleanField(default=False)
    updated = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [models.Index(fields=["test", "pending_tasks"])]

    def __str__(self) -> str:
        return f"DeduplicationTaskGroup(test={self.test_id}, remaining={self.pending_tasks})"

    def mark_complete_if_finished(self) -> None:
        if self.pending_tasks == 0 and not self.deduplication_complete:
            self.deduplication_complete = True
            self.completed_at = timezone.now()
            self.save(update_fields=["deduplication_complete", "completed_at"])

    def refresh_pending_tasks(self) -> None:
        """
        Recalculate the outstanding deduplication tasks for this group.

        The number of pending tasks is defined as the difference between the
        total number of findings belonging to the test and the number of
        findings that have already been processed (a record exists in
        ``ProcessedFinding``). Only findings that currently exist are
        considered; if a finding is deleted its corresponding processed
        record is effectively ignored when counting.

        This method also updates ``deduplication_complete`` and
        ``completed_at`` accordingly and synchronises the state back onto
        the ``Test``. ``started_at`` will be set if it has not yet been
        initialised. It is safe to call this method at any time as it
        always sets the counters based on the current database state.
        """
        test_id = self.test_id
        total_findings = Finding.objects.filter(test_id=test_id).count()
        existing_finding_ids = Finding.objects.filter(test_id=test_id).values_list(
            "id", flat=True
        )
        processed_count = ProcessedFinding.objects.filter(
            test_id=test_id, finding_id__in=existing_finding_ids
        ).count()
        pending = max(total_findings - processed_count, 0)
        updated_fields = []
        if self.pending_tasks != pending:
            self.pending_tasks = pending
            updated_fields.append("pending_tasks")
        if self.started_at is None:
            self.started_at = timezone.now()
            updated_fields.append("started_at")
        completed_now = (pending == 0)
        if completed_now and not self.deduplication_complete:
            self.deduplication_complete = True
            self.completed_at = timezone.now()
            updated_fields.extend(["deduplication_complete", "completed_at"])
        elif not completed_now and self.deduplication_complete:
            self.deduplication_complete = False
            self.completed_at = None
            updated_fields.extend(["deduplication_complete", "completed_at"])
        if updated_fields:
            self.save(update_fields=updated_fields)
        if completed_now:
            Test.objects.filter(pk=test_id, deduplication_complete=False).update(
                deduplication_complete=True
            )
        else:
            Test.objects.filter(pk=test_id, deduplication_complete=True).update(
                deduplication_complete=False
            )


class ProcessedFinding(models.Model):
    """Set which findings are considered to avoid double decrement"""
    test = models.ForeignKey(Test, on_delete=models.CASCADE)
    finding = models.ForeignKey(Finding, on_delete=models.CASCADE)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["test", "finding"], name="uniq_test_finding_processed"
            )
        ]
