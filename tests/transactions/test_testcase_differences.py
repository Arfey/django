import asyncio

from asgiref.sync import sync_to_async

from django.db import DatabaseError, connection, transaction
from django.test import TestCase, TransactionTestCase

from .models import Reporter


def run_invalid_query():
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM missing_table")


class SubtaskBehaviorTests:
    """The assertions describe the behavior outside a transaction."""

    async def test_two_subtasks_with_atomic(self):
        both_entered = asyncio.Barrier(2)

        async def create(name):
            async with transaction.atomic():
                await Reporter.objects.acreate(first_name=name)
                await both_entered.wait()

        async with asyncio.timeout(5), asyncio.TaskGroup() as tasks:
            tasks.create_task(create("Tintin"))
            tasks.create_task(create("Haddock"))
        self.assertEqual(await Reporter.objects.acount(), 2)

    async def test_handled_error_in_plain_subtask_keeps_atomic_write(self):
        entered = asyncio.Event()
        query_failed = asyncio.Event()

        async def with_atomic():
            async with transaction.atomic():
                await Reporter.objects.acreate(first_name="Tintin")
                entered.set()
                await query_failed.wait()

        async def without_atomic():
            await entered.wait()
            try:
                await sync_to_async(run_invalid_query)()
            except DatabaseError:
                pass  # The error is handled here.
            query_failed.set()

        async with asyncio.timeout(5), asyncio.TaskGroup() as tasks:
            tasks.create_task(with_atomic())
            tasks.create_task(without_atomic())
        self.assertEqual(await Reporter.objects.acount(), 1)

    async def test_plain_subtask_does_not_see_uncommitted_write(self):
        entered = asyncio.Event()
        checked = asyncio.Event()
        visible_rows = None

        async def with_atomic():
            async with transaction.atomic():
                await Reporter.objects.acreate(first_name="Tintin")
                entered.set()
                await checked.wait()

        async def without_atomic():
            nonlocal visible_rows
            await entered.wait()
            visible_rows = await Reporter.objects.acount()
            checked.set()

        async with asyncio.timeout(5), asyncio.TaskGroup() as tasks:
            tasks.create_task(with_atomic())
            tasks.create_task(without_atomic())
        self.assertEqual(visible_rows, 0)


class SubtaskBehaviorInTransactionTestCase(SubtaskBehaviorTests, TransactionTestCase):
    available_apps = ["transactions"]


class SubtaskBehaviorInTestCase(SubtaskBehaviorTests, TestCase):
    pass
