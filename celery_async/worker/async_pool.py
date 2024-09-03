import os
import sys
import asyncio
import time
import logging

from threading import Thread, Semaphore
from celery.concurrency.base import (
    BasePool,
    reraise,
    WorkerLostError,
    ExceptionInfo,
)

logger = logging.getLogger(__name__)


def loop_thread(loop, event):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(event.wait())


class AsyncIOPool(BasePool):
    signal_safe = True
    task_join_will_block = False

    def __init__(self, *args, **kwargs):
        """
        Keyword Args:
            timeout(int): Time in seconds that the pool will wait for tasks to complete
        """
        self.timeout = kwargs.get('timeout', 0)
        super().__init__(*args, **kwargs)

        self.loop = asyncio.new_event_loop()
        self.stop_event = asyncio.Event()
        self.loop_thread = Thread(target=loop_thread, args=(self.loop, self.stop_event))
        self.loop_thread.daemon = True

        self.semaphore = Semaphore(self.limit)
        self.active_tasks = {}

    def on_start(self):
        self.loop_thread.start()

    def on_stop(self):
        """Wait last tasks and close loop"""
        logger.debug("Pool stop")
        self._stop()

    def on_terminate(self):
        logger.info("Pool terminate")
        self._stop()

    def on_apply(self, target, args=None, kwargs=None, **opts):
        # parse_args
        (
            task_name,
            task_uuid,
            request,
            body,
            content_type,
            content_encoding,
        ) = args

        self.semaphore.acquire()

        target_coro = target(task_name, task_uuid, request, body, content_type, content_encoding)
        wrapped = self.target_wrapper(task_uuid, target_coro, **opts)
        self.active_tasks[task_uuid] = wrapped

        asyncio.run_coroutine_threadsafe(wrapped, self.loop)

    async def target_wrapper(
        self,
        target_id,
        target,
        callback=None,
        accept_callback=None,
        pid=None,
        getpid=os.getpid,
        monotonic=time.monotonic,
        **_
    ):
        """Apply function within pool context."""
        if accept_callback:
            accept_callback(pid or getpid(), monotonic())

        try:
            ret = await target
        except Exception:
            raise
        except BaseException as exc:
            try:
                reraise(WorkerLostError, WorkerLostError(repr(exc)), sys.exc_info()[2])
            except WorkerLostError:
                if callback:
                    callback(ExceptionInfo())
        else:
            if callback:
                callback((0, ret, monotonic()))
        finally:
            self.semaphore.release()
            self.active_tasks.pop(target_id, None)

    def _get_info(self):
        """
        Return configuration and statistics information. Subclasses should
        augment the data as required.
        Returns:
            dict[str, typing.Any]: The returned value must be JSON-friendly.
        """
        info = super()._get_info()
        info["tasks-running"] = len(self.active_tasks)
        return info

    def _stop(self):
        try:
            if self._wait_tasks_ends(self.timeout):
                return
            logger.warning("Pool was stopped due to timeout, some tasks may be lose")
        finally:
            self._terminate_loop()

    def _terminate_loop(self):
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.stop_event.set)
            self.loop_thread.join(self.timeout or 5)
        if not self.loop.is_closed():
            self.loop.close()

    def _wait_tasks_ends(self, timeout):
        if not timeout:
            return not self.active_tasks

        while timeout:
            time.sleep(1)
            if not self.active_tasks:
                return True
