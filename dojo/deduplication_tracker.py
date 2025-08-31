# dojo/deduplication_tracker.py
from __future__ import annotations

from django.db import models, transaction
from django.db.models import F
from django.db.models.signals import post_save, post_delete
from django.utils import timezone
from django.dispatch import receiver

from dojo.models import Test, Finding
from dojo.signals import finding_deduplicated


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


@receiver(finding_deduplicated)
def on_finding_deduplicated(sender, finding_id=None, test=None, **kwargs):
    """
    Reaction on finishing of deduplication of concrete finding
    - set finding as considered
    - reduce DeduplicationTaskGroup's pending_tasks
    - on 0 set completed
    """
    if not finding_id:
        return
    test_id = getattr(test, "id", None)
    if not test_id:
        try:
            test_id = Finding.objects.only("test_id").get(pk=finding_id).test_id
        except Finding.DoesNotExist:
            return
    _, created = ProcessedFinding.objects.get_or_create(
        test_id=test_id, finding_id=finding_id
    )
    if not created:
        return
    # Atomically decrement the pending count if positive
    TestDeduplicationProgress.objects.filter(
        test_id=test_id, pending_tasks__gt=0
    ).update(pending_tasks=F("pending_tasks") - 1)
    try:
        group = TestDeduplicationProgress.objects.get(test_id=test_id)
    except TestDeduplicationProgress.DoesNotExist:
        return
    group.refresh_pending_tasks()


# Additional signals to manage dynamic refresh of pending tasks


@receiver(post_save, sender=Test)
def create_dedup_group_on_test_save(sender, instance, created, **kwargs):
    """
    Ensure that a DeduplicationTaskGroup exists for every Test.

    When a new Test is created the group is initialised and its pending
    tasks are computed. For existing tests this signal can also be
    triggered via bulk operations (e.g. updating fields) so we refresh
    the counters to stay in sync.
    """
    group, _ = TestDeduplicationProgress.objects.get_or_create(test=instance)
    # Always recompute pending tasks to pick up any missing or newly added findings.
    group.refresh_pending_tasks()


@receiver(post_save, sender=Finding)
def refresh_on_finding_save(sender, instance, created, **kwargs):
    """
    Recalculate pending tasks when a Finding is created or updated.

    We refresh the DeduplicationTaskGroup for the finding's test to account
    for new findings or changes to the test association.
    """
    test_id = instance.test_id
    if not test_id:
        return
    # Ensure a DeduplicationTaskGroup exists for the test
    group, _ = TestDeduplicationProgress.objects.get_or_create(test_id=test_id)
    group.refresh_pending_tasks()


@receiver(post_delete, sender=Finding)
def refresh_on_finding_delete(sender, instance, **kwargs):
    """
    Update pending tasks when a Finding is deleted.

    Deleted findings should no longer contribute to the total number of tasks.
    """
    test_id = instance.test_id
    if not test_id:
        return
    try:
        group = TestDeduplicationProgress.objects.get(test_id=test_id)
    except TestDeduplicationProgress.DoesNotExist:
        return
    group.refresh_pending_tasks()
