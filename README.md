# celery_async

Async pool for celery worker
The pool provides a graceful stop while waiting for the current tasks to be completed

```python
from celery import Celery
from celery_async.task import AsyncTask

app = Celery('hello',
             broker='amqp://guest:guestpass@localhost/',
             task_cls=AsyncTask)


@app.task
async def some_task():
    print("Hello from async task")
```


```bash
celery --app=app worker -P async -c 100 
# -c max concurrent coroutines 
```