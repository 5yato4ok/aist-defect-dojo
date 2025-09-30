
import threading
from django.test import TransactionTestCase
from django.utils import timezone
from dojo.models import Product, Engagement, Test, Test_Type, Finding, Product_Type
from dojo.aist.models import TestDeduplicationProgress, ProcessedFinding
from dojo.signals import finding_deduplicated
import dojo.product.signals as product_signals
from django.db.models.signals import post_save

class ConcurrentDeduplicationTest(TransactionTestCase):
    def setUp(self):
        # Create basic DefectDojo objects
        product = Product.objects.create(
            name="Test Product", description="desc", prod_type_id=1
        )
        engagement = Engagement.objects.create(
            name="Engage",
            target_start=timezone.now(),
            target_end=timezone.now(),
            product=product,
        )
        test_type = Test_Type.objects.create(name="SAST")
        self.test = Test.objects.create(
            engagement=engagement,
            target_start=timezone.now(),
            target_end=timezone.now(),
            test_type=test_type,
            name="SAST Test",
        )
        # Deduplication progress should exist
        self.progress = TestDeduplicationProgress.objects.create(test=self.test)

        # Create 2 finding, duplicates
        self.f1 = Finding.objects.create(
            test=self.test,
            title="VulnA",
            date=timezone.now(),
            severity="High",
            description="desc"
        )
        self.f2 = Finding.objects.create(
            test=self.test,
            title="VulnA",
            date=timezone.now(),
            severity="High",
            description="desc"
        )
        # Init counters
        self.progress.refresh_pending_tasks()
        self.assertEqual(self.progress.pending_tasks, 2)

    def test_concurrent_dedupe_and_delete(self):
        def send_signal():
            # Assume f1 and f2 duplicates
            finding_deduplicated.send(
                sender=Finding, finding_id=self.f2.id, test=self.test
            )

        def delete_finding():
            self.f2.delete()

        # Start two actions in separate threads
        t1 = threading.Thread(target=send_signal)
        t2 = threading.Thread(target=delete_finding)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.progress.refresh_pending_tasks()

        # Check that there is no awaiting,and test is finished
        self.progress.refresh_from_db()
        self.assertEqual(self.progress.pending_tasks, 0)
        self.assertTrue(self.progress.deduplication_complete)
        self.test.refresh_from_db()
        self.assertTrue(self.test.deduplication_complete)

class DeletedProcessedFindingsTest(TransactionTestCase):
    def setUp(self):
        post_save.disconnect(
            receiver=product_signals.product_post_save,
            sender=Product,
        )
        self.prod_type = Product_Type.objects.create(name="PT for tests")
        product = Product.objects.create(name="P", prod_type=self.prod_type, description="d")
        engagement = Engagement.objects.create(
            name="E", target_start=timezone.now(), target_end=timezone.now(), product=product
        )
        ttype = Test_Type.objects.create(name="SAST")
        self.test = Test.objects.create(
            engagement=engagement, test_type=ttype,
            target_start=timezone.now(), target_end=timezone.now(), name="T"
        )
        self.grp = TestDeduplicationProgress.objects.create(test=self.test)

    def tearDown(self):
        post_save.connect(
            receiver=product_signals.product_post_save,
            sender=Product,
        )

    def test_deleted_processed_do_not_fake_complete(self):
        findings = [
            Finding.objects.create(test=self.test, title=f"A{i}", severity="High", date=timezone.now())
            for i in range(100)
        ]
        # пометим 60 как обработанные
        for f in findings[:60]:
            ProcessedFinding.objects.get_or_create(test_id=self.test.id, finding_id=f.id)

        # удалим эти 60
        for f in findings[:60]:
            f.delete()

        # осталось 40 живых находок без ProcessedFinding -> pending == 40
        self.grp.refresh_pending_tasks()
        self.grp.refresh_from_db()
        self.assertEqual(self.grp.pending_tasks, 40)
        self.assertFalse(self.grp.deduplication_complete)