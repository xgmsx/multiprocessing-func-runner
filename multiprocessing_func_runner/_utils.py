import typing
from time import sleep
from itertools import islice
from contextlib import contextmanager
from queue import Empty as EmptyError

from multiprocessing import Pool
from multiprocessing.context import TimeoutError as mp_TimeoutError
from multiprocessing.queues import Queue


class ExceptionKeyboardInterrupt(Exception):
    def __init__(self, message=None):
        super().__init__(message)


class ExceptionTimeoutError(mp_TimeoutError):
    def __init__(self, message=None):
        super().__init__(message)


def get_auto_chunk_size(iterable, pool_size: int) -> int:
    chunk_size = 0
    if len(iterable) != 0:
        chunk_size, extra = divmod(len(iterable), pool_size * 4)
        if extra:
            chunk_size += 1

    return chunk_size


def get_chunks_iter(iterable, chunk_size: int):
    it = iter(iterable)
    while 1:
        x = tuple(islice(it, chunk_size))
        if not x:
            return
        yield x


@contextmanager
def closing_pool(pool: Pool):
    with_terminate: typing.Optional[bool] = None
    try:
        with_terminate = yield pool
        if with_terminate is True:
            yield
    finally:
        if with_terminate is True:
            pool.terminate()
        else:
            pool.close()
        pool.join()


def force_terminate_pool(ctx_manager):
    ctx_manager.gen.send(True)


def init_pool_worker():
    import signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def check_timeout(workers: list, timeout: int, delay: int = 1):
    time_counter = 0
    while True:
        if all(worker.ready() for worker in workers):
            break
        if time_counter > timeout:
            raise ExceptionTimeoutError

        time_counter += delay
        sleep(delay)


def queue_worker_function(func: typing.Callable, queue: Queue, arg_name: str = None, kwargs: dict = None):

    def _execute_func(_func, _value, _arg_name: str = None, _kwargs: dict = None):
        _kwargs = kwargs if kwargs else dict()
        if arg_name:
            _kwargs[arg_name] = value
            return func(**_kwargs)
        else:
            return func(value, **_kwargs)

    results = list()

    while True:
        try:
            value = queue.get_nowait()
            result = _execute_func(func, value, arg_name, kwargs)
            results.append(result)
            queue.task_done()
        except EmptyError:
            break
        except KeyboardInterrupt:
            raise ExceptionKeyboardInterrupt

    return results
