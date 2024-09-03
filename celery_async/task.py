import asyncio

import celery
from celery import current_app as app
from celery.utils.functional import chunks
from celery.app.task import _task_stack, signature
from celery_async.utils import to_async


# celery.starmap not work with coroutine use async starmap
@app.task(name="celery.async_starmap")
async def async_starmap(task, it):
    task = signature(task, app=app).type
    return [await task(*item) for item in it]


class AsyncTask(app.Task):
    """This Task class may be aborted"""
    abstract = True

    async def __call__(self, *args, **kwargs):
        # need for correct work wit starmap or other primitives
        # original celery don`t push context for tasks in starmap
        _task_stack.push(self)
        if not self.request_stack.top:
            self.push_request(args=args, kwargs=kwargs)

        try:
            return await self.run(*args, **kwargs)
        finally:
            self.pop_request()
            _task_stack.pop()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.run = to_async(self.run)

    def chunks(self, it, n, **options):
        """
        Split many task in small chunks performed synchronously
        Args:
            it(list|tuple): List of argument for tasks
            n(int): chunk size
            options(dict): optional task arguments
        Returns:
            celery.group: group of chunks
        """
        return celery.group(
            (async_starmap.s(self.s(), part) for part in chunks(iter(it), n)),
            app=self._app,
            **options
        )
