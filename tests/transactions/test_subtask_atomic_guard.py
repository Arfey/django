"""
Evaluates a proposed rule: inside TestCase, raise an error when a subtask enters
an atomic block, so that users have to switch to TransactionTestCase.

The rule is prototyped with a mock. All tests here pass when each claim holds.
"""

import asyncio
from unittest import mock

from asgiref.sync import sync_to_async

from django.db import DatabaseError, connection, transaction
from django.test import TestCase, TransactionTestCase

from .models import Reporter

GUARD_MESSAGE = "Use TransactionTestCase to test atomic blocks in subtasks."


def reject_atomic_in_subtasks():
    """Reject atomic entry from any task but the current one (the test's task)."""
    root = asyncio.current_task()
    enter_async = transaction.Atomic._enter_async

    def guarded(self, task):
        blocks = transaction.get_connection(self.using).atomic_blocks
        only_testcase_blocks = blocks and all(b._from_testcase for b in blocks)
        if only_testcase_blocks and task is not root:
            raise transaction.TransactionManagementError(GUARD_MESSAGE)
        return enter_async(self, task)

    return mock.patch.object(transaction.Atomic, "_enter_async", guarded)


def run_invalid_query():
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM missing_table")


async def create_in_atomic():
    async with transaction.atomic():
        await Reporter.objects.acreate(first_name="Tintin")


async def atomic_subtask_and_plain_subtask_read():
    """gather(atomic, plain): the shape from the PR feedback."""
    entered = asyncio.Event()
    checked = asyncio.Event()

    async def with_atomic():
        async with transaction.atomic():
            await Reporter.objects.acreate(first_name="Tintin")
            entered.set()
            await checked.wait()

    async def without_atomic():
        await entered.wait()
        rows = await Reporter.objects.acount()
        checked.set()
        return rows

    async with asyncio.timeout(5), asyncio.TaskGroup() as tasks:
        tasks.create_task(with_atomic())
        plain = tasks.create_task(without_atomic())
    return plain.result()


async def atomic_in_test_task_and_plain_subtask_read():
    """The atomic block is in the test's task. Only the plain code is a subtask."""
    entered = asyncio.Event()
    checked = asyncio.Event()

    async def without_atomic():
        await entered.wait()
        rows = await Reporter.objects.acount()
        checked.set()
        return rows

    plain = asyncio.create_task(without_atomic())
    async with asyncio.timeout(5), transaction.atomic():
        await Reporter.objects.acreate(first_name="Tintin")
        entered.set()
        await checked.wait()
    return await plain


async def atomic_in_test_task_and_plain_subtask_error():
    entered = asyncio.Event()
    failed = asyncio.Event()

    async def without_atomic():
        await entered.wait()
        try:
            await sync_to_async(run_invalid_query)()
        except DatabaseError:
            pass
        failed.set()

    plain = asyncio.create_task(without_atomic())
    try:
        async with asyncio.timeout(5), transaction.atomic():
            await Reporter.objects.acreate(first_name="Tintin")
            entered.set()
            await failed.wait()
    finally:
        await plain


class GuardInTestCase(TestCase):
    def assertRejectedByGuard(self, raised):
        errors = raised.exception.exceptions
        self.assertEqual([str(error) for error in errors], [GUARD_MESSAGE])

    # What the rule fixes: silent differences become a clear error.

    async def test_catches_two_atomic_subtasks(self):
        both_entered = asyncio.Barrier(2)

        async def create(name):
            async with transaction.atomic():
                await Reporter.objects.acreate(first_name=name)
                await both_entered.wait()

        with reject_atomic_in_subtasks(), self.assertRaises(ExceptionGroup) as raised:
            async with asyncio.timeout(5), asyncio.TaskGroup() as tasks:
                tasks.create_task(create("Tintin"))
                tasks.create_task(create("Haddock"))
        self.assertEqual(str(raised.exception.exceptions[0]), GUARD_MESSAGE)

    async def test_without_guard_plain_subtask_silently_sees_uncommitted(self):
        self.assertEqual(await atomic_subtask_and_plain_subtask_read(), 1)

    async def test_catches_atomic_subtask_next_to_plain_subtask(self):
        async def run():
            async def with_atomic():
                async with transaction.atomic():
                    pass

            async def without_atomic():
                await asyncio.sleep(0)

            async with asyncio.timeout(5), asyncio.TaskGroup() as tasks:
                tasks.create_task(with_atomic())
                tasks.create_task(without_atomic())

        with reject_atomic_in_subtasks(), self.assertRaises(ExceptionGroup) as raised:
            await run()
        self.assertRejectedByGuard(raised)

    # What the rule breaks: tests that give the production result today.

    async def test_single_subtask_works_without_guard(self):
        await asyncio.create_task(create_in_atomic())
        self.assertEqual(await Reporter.objects.acount(), 1)

    async def test_guard_rejects_single_subtask(self):
        with reject_atomic_in_subtasks():
            with self.assertRaisesMessage(
                transaction.TransactionManagementError, GUARD_MESSAGE
            ):
                await asyncio.create_task(create_in_atomic())

    async def test_sequential_subtasks_work_without_guard(self):
        for _ in range(2):
            await asyncio.create_task(create_in_atomic())
        self.assertEqual(await Reporter.objects.acount(), 2)

    # What the rule misses: no atomic in a subtask, the difference stays silent.

    async def test_guard_misses_uncommitted_read_when_atomic_is_in_test_task(self):
        with reject_atomic_in_subtasks():
            rows = await atomic_in_test_task_and_plain_subtask_read()
        self.assertEqual(rows, 1)  # 0 under TransactionTestCase.

    async def test_guard_misses_error_when_atomic_is_in_test_task(self):
        with reject_atomic_in_subtasks():
            with self.assertRaisesMessage(
                DatabaseError, "current transaction is aborted"
            ):
                await atomic_in_test_task_and_plain_subtask_error()
        self.assertEqual(await Reporter.objects.acount(), 0)  # 1 under TTC.


class ProductionBehavior(TransactionTestCase):
    """Controls: the same scenarios outside a transaction."""

    available_apps = ["transactions"]

    async def test_single_subtask(self):
        await asyncio.create_task(create_in_atomic())
        self.assertEqual(await Reporter.objects.acount(), 1)

    async def test_sequential_subtasks(self):
        for _ in range(2):
            await asyncio.create_task(create_in_atomic())
        self.assertEqual(await Reporter.objects.acount(), 2)

    async def test_plain_subtask_read_with_atomic_subtask(self):
        self.assertEqual(await atomic_subtask_and_plain_subtask_read(), 0)

    async def test_plain_subtask_read_with_atomic_in_test_task(self):
        self.assertEqual(await atomic_in_test_task_and_plain_subtask_read(), 0)

    async def test_plain_subtask_error_with_atomic_in_test_task(self):
        await atomic_in_test_task_and_plain_subtask_error()
        self.assertEqual(await Reporter.objects.acount(), 1)
