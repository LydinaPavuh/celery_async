# celery_async

Async pool for celery worker  
The pool provides a graceful stop while waiting for the current tasks to be completed  
Pool may works with non async tasks, but these tasks blocks events loop for other tasks  

```python
from celery import Celery
from celery_async import AsyncTask

app = Celery('hello',
             broker='amqp://guest:guestpass@localhost/',
             task_cls=AsyncTask)


@app.task
async def some_task():
    print("Hello from async task")



some_task.delay()
some_task.starmap([] for i in range(10)).delay()
some_task.chunks([[] for i in range(10)], 2).delay()
```


```bash
celery --app=app worker -P async -c 100 
# -c max concurrent coroutines 
```