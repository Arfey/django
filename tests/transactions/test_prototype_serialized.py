"""PROTOTYPE: serialized async atomic blocks."""

import asyncio

from django.db import transaction
from django.db.models.signals import post_save
from django.test import TestCase, TransactionTestCase

from .models import Reporter


class SerializedScenarios:
    async def test_two_atomic_subtasks_are_serialized(self):
        events = []

        async def create(name):
            async with transaction.atomic():
                events.append(f"enter {name}")
                await Reporter.objects.acreate(first_name=name)
                await asyncio.sleep(0.05)
                events.append(f"exit {name}")

        async with asyncio.timeout(5):
            await asyncio.gather(create("Tintin"), create("Haddock"))
        self.assertEqual(
            events, ["enter Tintin", "exit Tintin", "enter Haddock", "exit Haddock"]
        )
        self.assertEqual(await Reporter.objects.acount(), 2)

    async def test_plain_sibling_waits_and_sees_committed_write(self):
        entered = asyncio.Event()

        async def with_atomic():
            async with transaction.atomic():
                await Reporter.objects.acreate(first_name="Tintin")
                entered.set()
                await asyncio.sleep(0.05)

        async def without_atomic():
            await entered.wait()
            return await Reporter.objects.acount()

        async with asyncio.timeout(5):
            _, rows = await asyncio.gather(with_atomic(), without_atomic())
        self.assertEqual(rows, 1)

    async def test_child_task_nests_in_parent_block(self):
        async def child():
            async with transaction.atomic():
                await Reporter.objects.acreate(first_name="Haddock")

        try:
            async with asyncio.timeout(5), transaction.atomic():
                await Reporter.objects.acreate(first_name="Tintin")
                await asyncio.create_task(child())
                raise ValueError
        except ValueError:
            pass
        self.assertEqual(await Reporter.objects.acount(), 0)

    async def test_parent_query_waits_for_child_block(self):
        events = []
        child_entered = asyncio.Event()

        async def child():
            async with transaction.atomic():
                child_entered.set()
                await Reporter.objects.acreate(first_name="Haddock")
                await asyncio.sleep(0.05)
                events.append("child exit")
                raise ValueError  # Rolls back only the child's savepoint.

        async with asyncio.timeout(5), transaction.atomic():
            task = asyncio.create_task(child())
            await child_entered.wait()
            # Must not run inside the child's savepoint.
            await Reporter.objects.acreate(first_name="Tintin")
            events.append("parent query")
            with self.assertRaises(ValueError):
                await task
        self.assertEqual(events, ["child exit", "parent query"])
        self.assertEqual(
            [r.first_name async for r in Reporter.objects.all()], ["Tintin"]
        )

    async def test_async_receiver_with_atomic(self):
        async def receiver(sender, instance, created, **kwargs):
            if instance.first_name == "Tintin":
                async with transaction.atomic():
                    await Reporter.objects.acreate(first_name="Haddock")

        post_save.connect(receiver, sender=Reporter)
        self.addCleanup(post_save.disconnect, receiver, sender=Reporter)
        async with asyncio.timeout(5), transaction.atomic():
            await Reporter.objects.acreate(first_name="Tintin")
        self.assertEqual(await Reporter.objects.acount(), 2)

    async def test_waiting_for_sibling_inside_block_deadlocks(self):
        """The cost: a block that waits on a sibling's query never finishes."""
        sibling_done = asyncio.Event()

        async def with_atomic():
            async with transaction.atomic():
                await sibling_done.wait()

        async def sibling():
            await asyncio.sleep(0.01)
            await Reporter.objects.acount()
            sibling_done.set()

        with self.assertRaises(TimeoutError):
            async with asyncio.timeout(0.5):
                await asyncio.gather(with_atomic(), sibling())


class SerializedInTestCase(SerializedScenarios, TestCase):
    pass


class SerializedInTransactionTestCase(SerializedScenarios, TransactionTestCase):
    available_apps = ["transactions"]
