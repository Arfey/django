"""Edge cases found while reviewing PR #22074."""

import asyncio
import threading

from asgiref.sync import async_to_sync, sync_to_async

from django.db import transaction
from django.db.models.signals import post_delete, post_save
from django.test import TransactionTestCase

from .models import Reporter


class SyncToAsyncEdgeCases(TransactionTestCase):
    available_apps = ["transactions"]

    async def test_sync_atomic_in_child_task_sync_to_async_raises(self):
        def create():
            with transaction.atomic():
                Reporter.objects.create(first_name="Haddock")

        with self.assertRaises(transaction.TransactionManagementError):
            async with transaction.atomic():
                await asyncio.create_task(sync_to_async(create)())

    async def test_async_atomic_in_async_to_sync_in_sync_atomic_raises(self):
        async def create():
            async with transaction.atomic():
                await Reporter.objects.acreate(first_name="Haddock")

        def run():
            with transaction.atomic():
                async_to_sync(create)()

        with self.assertRaises(transaction.TransactionManagementError):
            await sync_to_async(run)()

    async def test_child_sync_atomic_write_lost_on_parent_savepoint_rollback(self):
        def create():
            with transaction.atomic():
                Reporter.objects.create(first_name="Haddock")

        async with asyncio.timeout(5), transaction.atomic():
            try:
                async with transaction.atomic():
                    child = asyncio.create_task(sync_to_async(create)())
                    await asyncio.sleep(0.1)
                    raise ValueError
            except ValueError:
                pass
            await child
        # The child's atomic block succeeded, so its write should survive.
        self.assertEqual(await Reporter.objects.acount(), 1)


class SignalEdgeCases(TransactionTestCase):
    available_apps = ["transactions"]

    def connect(self, signal, receiver):
        signal.connect(receiver, sender=Reporter)
        self.addCleanup(signal.disconnect, receiver, sender=Reporter)

    async def test_async_receiver_atomic_with_outer_async_atomic(self):
        async def receiver(sender, instance, created, **kwargs):
            if instance.first_name == "Tintin":
                async with transaction.atomic():
                    await Reporter.objects.acreate(first_name="Haddock")

        self.connect(post_save, receiver)
        async with asyncio.timeout(5), transaction.atomic():
            await Reporter.objects.acreate(first_name="Tintin")
        self.assertEqual(await Reporter.objects.acount(), 2)

    async def test_async_receiver_atomic_in_internal_sync_atomic(self):
        async def receiver(sender, instance, **kwargs):
            async with transaction.atomic():
                await Reporter.objects.acreate(first_name="Haddock")

        self.connect(post_delete, receiver)
        reporter = await Reporter.objects.acreate(first_name="Tintin")
        async with asyncio.timeout(5):
            await reporter.adelete()  # delete() uses atomic() internally.
        self.assertEqual(await Reporter.objects.acount(), 1)

    async def test_async_receiver_atomic_in_internal_atomic_in_async_atomic(self):
        async def receiver(sender, instance, **kwargs):
            async with transaction.atomic():
                await Reporter.objects.acreate(first_name="Haddock")

        self.connect(post_delete, receiver)
        reporter = await Reporter.objects.acreate(first_name="Tintin")
        async with asyncio.timeout(5), transaction.atomic():
            await reporter.adelete()
        self.assertEqual(await Reporter.objects.acount(), 1)

    async def test_sync_receiver_runs_on_same_thread_as_outside_atomic(self):
        """Sync receivers don't run on a new thread."""
        threads = []

        def receiver(sender, instance, **kwargs):
            threads.append(threading.current_thread())

        self.connect(post_save, receiver)
        await Reporter.objects.acreate(first_name="Tintin")
        async with asyncio.timeout(5), transaction.atomic():
            await Reporter.objects.acreate(first_name="Haddock")
        self.assertIs(threads[0], threads[1])


class OrmInternalAtomicEdgeCases(TransactionTestCase):
    available_apps = ["transactions"]

    async def test_internal_atomics_in_owner_task(self):
        async with asyncio.timeout(5), transaction.atomic():
            await Reporter.objects.abulk_create([Reporter(first_name="A")])
            await Reporter.objects.aget_or_create(first_name="B")
            await Reporter.objects.aupdate_or_create(first_name="C")
            reporter = await Reporter.objects.acreate(first_name="D")
            await reporter.adelete()
        self.assertEqual(await Reporter.objects.acount(), 3)

    async def test_internal_atomics_in_child_tasks(self):
        async with asyncio.timeout(5), transaction.atomic():
            await asyncio.gather(
                Reporter.objects.abulk_create([Reporter(first_name="A")]),
                Reporter.objects.aget_or_create(first_name="B"),
                Reporter.objects.aupdate_or_create(first_name="C"),
            )
        self.assertEqual(await Reporter.objects.acount(), 3)

    async def test_internal_atomic_in_child_task_rolled_back_with_parent(self):
        try:
            async with asyncio.timeout(5), transaction.atomic():
                await asyncio.create_task(
                    Reporter.objects.aget_or_create(first_name="B")
                )
                raise ValueError
        except ValueError:
            pass
        self.assertEqual(await Reporter.objects.acount(), 0)
