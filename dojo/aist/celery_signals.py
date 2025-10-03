from __future__ import annotations

from celery.signals import worker_process_init
from .logging_transport import install_global_redis_log_handler

from django.db import models, transaction
from django.db.models import F
from django.db.models.signals import post_save, post_delete
from django.utils import timezone
from django.dispatch import receiver

from dojo.models import Test, Finding
from dojo.signals import finding_deduplicated
from .models import ProcessedFinding, TestDeduplicationProgress

@worker_process_init.connect
def setup_logging_on_worker(**kwargs):
    pass
    # install handler once per worker process
    #install_global_redis_log_handler()


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