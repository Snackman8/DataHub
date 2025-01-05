from functools import wraps
import hashlib
import inspect
import logging
import os
import pandas as pd
import traceback
import time


def _generate_cache_filename(func_name, kwargs):
    """
    Generate a cache filename based on function name and parameters.

    Args:
        func_name (str): The name of the function.
        kwargs (dict): The function arguments as key-value pairs.

    Returns:
        str: A sanitized cache filename.
    """
    params = [f"{k}={str(v)}" for k, v in sorted(kwargs.items())]
    param_str = '&'.join(params)
    if len(param_str) > 250:
        ks = ','.join(sorted(kwargs.keys()))
        vs = ','.join([str(kwargs[k]) for k in sorted(kwargs.keys())])
        param_str = '_params=' + hashlib.md5(ks.encode('ascii')).hexdigest() + \
                    '_' + hashlib.md5(vs.encode('ascii')).hexdigest()
    return f"{func_name}?{param_str}.pickle.gz"


def _sanitize_path_component(component):
    """
    Sanitize a string for use in a file path by replacing reserved characters.

    Args:
        component (str): The string to sanitize.

    Returns:
        str: A sanitized string.
    """
    reserved_chars = '\\/?%*:|<>,;'
    for rc in reserved_chars:
        component = component.replace(rc, '__')
    return component


def _write_to_cache(df, cache_path):
    """
    Write a DataFrame to a compressed cache file.

    Args:
        df (pd.DataFrame): The DataFrame to cache.
        cache_path (str): The file path for the cache.
    """
    if not os.path.exists(os.path.dirname(cache_path)):
        os.makedirs(os.path.dirname(cache_path))
    df.to_pickle(cache_path, compression={"method": "gzip", "compresslevel": 1, "mtime": 1})
    logging.info(f"Cache written to: {cache_path}")


def cacheable(cache_dir, filename, lag_params=[], lag_from_utc_now=None):
    """
    Decorator to enable caching for data-fetching functions.

    Args:
        cache_dir (str): Root directory for cache storage.
        filename (str): Filename of the script (e.g., __file__), used for cache subdirectories.
        lag_params (list): List of parameter names to check against the lag constraint.
        lag_from_utc_now (pd.Timedelta): A timedelta specifying the lag window for caching.
        nocache (bool, optional): If True, bypasses the cache entirely. Default is False.
        updatecache (bool, optional): If True, forces a cache update even if a cached result exists. Default is False.

    Returns:
        Callable: The decorated function with caching applied.

    Raises:
        Exception: If an unexpected error occurs in the function or caching process.
    """
    def decorator(func):
        @wraps(func)
        def new_func(*args, **kwargs):
            try:
                # Transfer args to kwargs
                bound_args = inspect.signature(func).bind(*args, **kwargs)
                bound_args.apply_defaults()
                kwargs = bound_args.arguments.copy()

                nocache = kwargs.pop("nocache", False)
                updatecache = kwargs.pop("updatecache", False)

                # Skip caching if `nocache` is True
                if nocache:
                    logging.info("Skipping cache due to nocache flag.")
                    return func(*args, **kwargs)

                # Check if cacheable based on lag
                cacheable = True
                if lag_params and lag_from_utc_now:
                    for param in lag_params:
                        param_value = kwargs.get(param)
                        if param_value and (
                            pd.Timestamp.utcnow().tz_localize(None) - pd.to_datetime(param_value).tz_localize(None)
                        ) <= lag_from_utc_now:
                            cacheable = False
                            break

                # Generate cache path
                cache_filename = _generate_cache_filename(func.__name__, kwargs)
                cache_path = os.path.join(
                    cache_dir,
                    _sanitize_path_component(os.path.splitext(os.path.basename(filename))[0]),
                    _sanitize_path_component(cache_filename),
                )

                # Read from cache if applicable
                if cacheable and not updatecache:
                    if os.path.exists(cache_path):
                        logging.info(f"Reading from cache: {cache_path}")
                        return pd.read_pickle(cache_path, compression="gzip")
                    else:
                        logging.info(f"Cache miss: {cache_path}")

                # Call the original function
                df = func(*args, **kwargs)

                # Write to cache if applicable
                if cacheable and isinstance(df, pd.DataFrame):
                    start_time = time.time()
                    _write_to_cache(df, cache_path)
                    logging.info(f"Cache written in {time.time() - start_time:.2f}s")

                return df
            except (OSError, IOError) as e:
                logging.error(f"Cache operation failed: {e}. Skipping cache.")
                return func(*args, **kwargs)
            except Exception as e:
                logging.error(traceback.format_exc())
                raise
        return new_func
    return decorator
