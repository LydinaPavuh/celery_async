import sys
from celery.concurrency import ALIASES
from celery import _find_option_with_arg
from celery.__main__ import main as main_
from celery_async.worker.monkey import monkey


ASYNCIO_POOL_TYPE = "async"
ALIASES[ASYNCIO_POOL_TYPE] = "celery_async.worker.async_pool:AsyncIOPool"


def main():
    argv = sys.argv
    short_opts = ['-P']
    long_opts = ['--pool']

    try:
        pool = _find_option_with_arg(argv, short_opts, long_opts)
    except KeyError:
        pass
    else:
        if pool == ASYNCIO_POOL_TYPE:
            monkey()

    main_()

