def monkey():
    from celery.app import trace
    from celery_async.worker.async_tracer import (
        trace_task_ret_async,
        build_async_tracer,
        trace_task_async,
        fast_trace_task_async,
    )

    # monkeypatch
    trace.trace_task_ret = trace_task_ret_async
    trace.build_tracer = build_async_tracer
    trace.trace_task = trace_task_async
    trace.fast_trace_task = fast_trace_task_async