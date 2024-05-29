"""Module with general utilities."""

import os
import time
import gc
from functools import wraps
from typing import Any, Callable
import psutil
import logging
import inspect

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


def measure_memory_leak(func_call):
    """Wrap around a function to track the memory leak by the function.

    :param func: Function

    .. note:: Usage as a decorator before a function -> @measure_memory_leak

    """

    @wraps(func_call)
    def _f(*args, **kwargs):
        # Get the current process and initial memory usage
        process = psutil.Process(os.getpid())
        rss_by_psutil_start = process.memory_info().rss / (1024 * 1024)

        # Call the original function
        result = func_call(*args, **kwargs)

        # Final memory usage after the function execution
        rss_by_psutil_end = process.memory_info().rss / (1024 * 1024)

        # Explicitly calling garbage collection
        gc.collect()
        diff = rss_by_psutil_end - rss_by_psutil_start

        # Get caller information
        caller = inspect.stack()[1]
        caller_info = f"{caller.filename} at line {caller.lineno} in {caller.function}"

        # Log the memory usage and the caller information
        logging.info(
            f"Memory change after executing {func_call.__name__}: {diff:.2f} MB (Called from {caller_info})"
        )

        return result

    return _f
