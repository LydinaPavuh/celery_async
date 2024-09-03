from celery._state import connect_on_app_finalize
from celery import signature


@connect_on_app_finalize
def add_accumulate_task(app):
    # celery.starmap not work with coroutine use async starmap
    @app.task(name="celery.async_starmap")
    async def async_starmap(task, it):
        task = signature(task, app=app).type
        return [await task(*item) for item in it]
