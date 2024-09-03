import sys
import asyncio
from celery.app.trace import (
    TraceInfo,
    RETRY,
    FAILURE,
    ExceptionInfo,
    get_pickleable_exception,
    get_pickleable_etype,
    signals,
    info,
    LOG_RETRY,
    get_task_name,
)


class AsyncTraceInfo(TraceInfo):
    async def handle_error_state(self, task, req, eager=False, call_errbacks=True):
        store_errors = not eager
        if task.ignore_result:
            store_errors = task.store_errors_even_if_ignored

        if self.state is RETRY:
            return await self.handle_retry(
                task, req, store_errors=store_errors, call_errbacks=call_errbacks
            )
        elif self.state is FAILURE:
            return await self.handle_failure(
                task, req, store_errors=store_errors, call_errbacks=call_errbacks
            )
        else:
            raise ValueError("Unknown state")

    async def handle_failure(self, task, req, store_errors=True, call_errbacks=True):
        """Handle exception."""
        _, _, tb = sys.exc_info()
        try:
            exc = self.retval
            # make sure we only send pickleable exceptions back to parent.
            einfo = ExceptionInfo()
            einfo.exception = get_pickleable_exception(einfo.exception)
            einfo.type = get_pickleable_etype(einfo.type)

            task.backend.mark_as_failure(
                req.id,
                exc,
                einfo.traceback,
                request=req,
                store_result=store_errors,
                call_errbacks=call_errbacks,
            )

            if asyncio.iscoroutinefunction(task.on_failure):
                await task.on_failure(exc, req.id, req.args, req.kwargs, einfo)
            else:
                task.on_failure(exc, req.id, req.args, req.kwargs, einfo)

            signals.task_failure.send(
                sender=task,
                task_id=req.id,
                exception=exc,
                args=req.args,
                kwargs=req.kwargs,
                traceback=tb,
                einfo=einfo,
            )
            self._log_error(task, req, einfo)
            return einfo
        finally:
            del tb

    async def handle_reject(self, task, req, **kwargs):
        self._log_error(task, req, ExceptionInfo())

    async def handle_ignore(self, task, req, **kwargs):
        self._log_error(task, req, ExceptionInfo())

    async def handle_retry(self, task, req, store_errors=True, **kwargs):
        """Handle retry exception."""
        # the exception raised is the Retry semi-predicate,
        # and it's exc' attribute is the original exception raised (if any).
        type_, _, tb = sys.exc_info()
        try:
            reason = self.retval
            einfo = ExceptionInfo((type_, reason, tb))
            if store_errors:
                task.backend.mark_as_retry(
                    req.id,
                    reason.exc,
                    einfo.traceback,
                    request=req,
                )

            if asyncio.iscoroutinefunction(task.on_retry):
                await task.on_retry(reason.exc, req.id, req.args, req.kwargs, einfo)
            else:
                task.on_retry(reason.exc, req.id, req.args, req.kwargs, einfo)

            signals.task_retry.send(
                sender=task, request=req, reason=reason, einfo=einfo
            )
            info(
                LOG_RETRY,
                {
                    "id": req.id,
                    "name": get_task_name(req, task.name),
                    "exc": str(reason),
                },
            )
            return einfo
        finally:
            del tb
