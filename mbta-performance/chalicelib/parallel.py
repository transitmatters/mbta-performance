from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd


def make_parallel(single_func, THREAD_COUNT=5):
    """Wrap a function so it runs concurrently over an iterable of inputs.

    Similar to a decorator but preserves the original function. The wrapped
    function's first positional argument is replaced by an iterable; each
    item in the iterable is passed to a separate thread.

    Example::

        parallel_fetch = make_parallel(fetch_day)
        results = parallel_fetch([date1, date2, date3], extra_arg)

    Args:
        single_func: The function to parallelize. Its first parameter is the
            item to multiplex; all remaining args/kwargs are forwarded as-is.
        THREAD_COUNT: Maximum number of concurrent threads. Defaults to 5.

    Returns:
        A new function that accepts an iterable as its first argument and
        returns a flat list of all results.
    """

    def parallel_func(iterable, *args, **kwargs):
        futures = []
        with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
            for i in iterable:
                futures.append(executor.submit(single_func, i, *args, **kwargs))
            as_completed(futures)
        results = [val for future in futures for val in future.result()]
        return results

    return parallel_func


def date_range(start, end):
    """Return a daily DatetimeIndex from start to end, inclusive.

    Args:
        start: Start date (any type accepted by pandas.date_range).
        end: End date (any type accepted by pandas.date_range).

    Returns:
        A pandas DatetimeIndex with daily frequency.
    """
    return pd.date_range(start, end)


def month_range(start, end):
    """Return a month-end DatetimeIndex covering every month between start and end.

    Uses a daily resample rather than a direct monthly date_range to ensure that
    edge cases like Jan 31 – Feb 1 produce both months in the result.

    Args:
        start: Start date (any type accepted by pandas.date_range).
        end: End date (any type accepted by pandas.date_range).

    Returns:
        A pandas DatetimeIndex with month-end frequency, containing one entry
        per calendar month spanned by [start, end].
    """
    dates = pd.date_range(start, end, freq="1D", inclusive="both")
    series = pd.Series(0, index=dates)
    months = series.resample("1M").sum().index
    return months
