"""Module with general utilities."""

import os
import time
import gc
from functools import wraps
from typing import Any, Callable
import psutil
import logging

gen_utils_log = logging.getLogger("gen_utils")
logging.basicConfig(level=logging.INFO)


def measure_time(func_call: Callable) -> Any:
    """Wrap around a function to track the time taken by the function.

    :param func: Function

    .. note:: Usage as a decorator before a function -> @timer

    """

    @wraps(
        func_call
    )  # Required to get documentation for functions using this decorator
    def _f(*args: Any, **kwargs: Any) -> Any:
        before = time.perf_counter()
        r_v = func_call(*args, **kwargs)
        after = time.perf_counter()

        time_taken = round((after - before) / 60, 2)
        gen_utils_log.info(f"Elapsed time for {func_call}: {time_taken} minutes")
        return r_v

    return _f


def measure_memory_leak(func_call: Callable):  # noqa
    """Wrap around a function to track the memory leak by the function.

    :param func: Function

    .. note:: Usage as a decorator before a function -> @timer

    """

    @wraps(func_call)
    def _f(*args, **kwargs) -> Any:
        process = psutil.Process(os.getpid())
        rss_by_psutil_start = process.memory_info().rss / (1024 * 1024)
        result = func_call(*args, **kwargs)
        rss_by_psutil_end = process.memory_info().rss / (1024 * 1024)
        gc.collect()
        diff = rss_by_psutil_end - rss_by_psutil_start
        gen_utils_log.info(f"Executing {func_call} leaked {diff:1.2f} MB")
        return result

    return _f
