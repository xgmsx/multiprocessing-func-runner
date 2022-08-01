from typing import Callable, Iterable
from functools import partial
from multiprocessing import Pool, Manager

import multiprocessing_func_runner._utils as mfr_utils


__all__ = ['run_function', 'run_function_chunks', 'unpack_results']


def run_function(
        func: Callable, iterable: Iterable,
        arg_name: str = None, kwargs: dict = None,
        pool_size: int = None, timeout: int = None) -> list:
    """Run function in multiprocessing

    Args:
        func: executed function
        iterable: collection of values (list, set, tuple etc)

        arg_name (optional): name of argument for getting value from 'iterable'. First argument if empty
        kwargs (optional): argument dictionary
        pool_size (optional): count of running processes
        timeout (optional): seconds before timeout

    Returns:
        list: list of results

    Raises:
        RuntimeError: If some error occurred
        TimeoutError: If the time has expired

    Examples:
        >>> print(run_function(abs, (-10, -5, 0, 5, 10), pool_size=1))
        [10, 5, 0, 5, 10]

        >>> print(run_function(round, (0.333, 0.222, 0.111), pool_size=1, kwargs={'ndigits': 1}))
        [0.3, 0.2, 0.1]
    """

    manager = Manager()
    queue = manager.Queue()
    [queue.put(elem) for elem in iterable]
    if queue.empty():
        return []

    pool_size = max(1, pool_size if pool_size else 0)
    pool = Pool(processes=pool_size, initializer=mfr_utils.init_pool_worker)

    ctx_manager = mfr_utils.closing_pool(pool)
    with ctx_manager as pool:
        try:

            func_partial = partial(mfr_utils.queue_worker_function, func, queue, arg_name=arg_name, kwargs=kwargs)
            workers = [pool.apply_async(func_partial) for _ in range(pool_size)]

            if timeout:
                mfr_utils.check_timeout(workers, timeout)

            results = [worker.get() for worker in workers]
            return unpack_results(results)

        except mfr_utils.ExceptionTimeoutError:
            mfr_utils.force_terminate_pool(ctx_manager)
            raise TimeoutError(f"{func.__name__}() stopped by timeout {timeout} seconds")
        except (mfr_utils.ExceptionKeyboardInterrupt, KeyboardInterrupt):
            mfr_utils.force_terminate_pool(ctx_manager)
            raise KeyboardInterrupt(f"{func.__name__}() stopped by user")
        except Exception as e:
            mfr_utils.force_terminate_pool(ctx_manager)
            raise RuntimeError(f"{func.__name__}() stopped with error <{type(e).__name__}>")


def run_function_chunks(
        func: Callable, iterable: Iterable, chunk_size: int = None,
        arg_name: str = None, kwargs: dict = None,
        pool_size: int = None, timeout: int = None):
    """Run function in multiprocessing in chunks

    Split values in the "iterable" argument into chunks and execute in multiprocessing

    Args:
        func: Executed function
        iterable: collection of values (list, set, tuple etc)
        chunk_size: count of values in one chunk

        arg_name (optional): name of argument for getting value from 'iterable'. First argument if empty
        kwargs (optional): argument dictionary
        pool_size (optional): count of running processes
        timeout (optional): seconds before timeout

    Returns:
        list: list of results

    Raises:
        RuntimeError: If some error occurred
        TimeoutError: If the time has expired

    Examples:
        >>> print(run_function_chunks(sum, (1, 2, 3, 4, 5, 6, 7), pool_size=1, chunk_size=2))
        [3, 7, 11, 7]
    """

    pool_size = max(1, pool_size if pool_size else 0)

    if chunk_size is None:
        chunk_size = mfr_utils.get_auto_chunk_size(iterable, pool_size)
    else:
        chunk_size = chunk_size

    chunks = mfr_utils.get_chunks_iter(iterable, chunk_size)

    results = run_function( func, chunks, arg_name, kwargs, pool_size, timeout)

    return results


def unpack_results(lst: list) -> list:
    """Expands a list of lists into a single-level flat list

    Examples:
        >>> ll = [[1, 2, 3], [4, 5], [6]]
        >>> print(unpack_results(ll))
        [1, 2, 3, 4, 5, 6]
    """
    return [value for lst_lvl2 in lst for value in lst_lvl2]
