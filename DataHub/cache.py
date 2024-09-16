from functools import wraps
import hashlib
import inspect
import io
import logging
import os
import time
import pandas as pd
import traceback


def decode_cache_to_df(result):
    # check if this is already a pickle
    if not isinstance(result, pd.DataFrame):
        return pd.read_pickle(result, compression='gzip')
    return result


def cacheable(cache_dir, filename, lag_params=[], lag_from_utc_now=None):
    """ decorator to enable caching for data queries

        Additional parameters of nocache to bypass the cache and updatecache to force a cache update can
        be passed in.

        Args:
            cache_dir - root dir for the cache, i.e. /srv/DataHub_Cache
            filename - filename of the file containing the code for the query, usually the __file__ variable is used
            lag_param - list of parameters to check lag aginast, i.e. ["end_date"]
            lag_from_utc_now - a timedelta specifying the lag for caching.  i.e. timedelta(days=2) means nothing within the last two days will be cached
    """
    def decorator(func):
        """ decorator function """
        @wraps(func)
        def new_func(*args, **kwargs):
            """
                nocache
                updatecache
            """
            try:
                """ internal new func for the decorator """
                # do not bother if nocache is set
                cacheable = False
                nocache = kwargs.pop('nocache', False)
                updatecache = kwargs.pop('updatecache', False)

                # transfer args to kwargs
                sig = inspect.signature(func)
                param_names = list(sig.parameters)
                for i in range(0, len(args)):
                    kwargs[param_names[i]] = args[i]
                args = []

                if not nocache or updatecache:
                    # loop through the lag_aarams
                    cacheable = True
                    for param_name in lag_params:
                        param_default = sig.parameters[param_name].default
                        param_value = kwargs.get(param_name, param_default)

                        # check if we are inside the lag
                        if param_value:
                            if (pd.Timestamp.utcnow().tz_localize(None) - pd.to_datetime(param_value)) <= lag_from_utc_now:
                                cacheable = False

                # check if we can read from the cache
                if cacheable:
                    # build the cache_path
                    subpath = os.path.splitext(os.path.split(filename)[1])[0]

                    # build params traditional way
                    params = []
                    for k in sorted(kwargs.keys()):
                        v = kwargs[k]
                        for rc in '\\/?%*:|<>,;':
                            k = k.replace(rc, '__')
                            v = v.replace(rc, '__')
                        params.append(f"""{k}={v}""")
                    s = '&'.join(params)
                    cache_filename = func.__name__ + '?' + s + ".pickle.gz"

                    # check if filename is too long
                    if len(cache_filename) > 250:
                        # filename too long due to parameters, switch to shortened style
                        ks = []
                        vs = []
                        for k in sorted(kwargs.keys()):
                            ks.append(k)
                            vs.append(str(kwargs[k]))
                        param_str = ('_params=' + hashlib.md5((','.join(ks)).encode('ascii')).hexdigest() +
                                     '_' + hashlib.md5((','.join(vs)).encode('ascii')).hexdigest())
                        cache_filename = func.__name__ + '?' + param_str + ".pickle.gz"

                    # normalize cachepath
                    cache_path = os.path.join(cache_dir, subpath, cache_filename).replace(' ', '_').replace('&', '_').replace('?', '_')
                    if not updatecache:
                        if os.path.exists(cache_path):
                            logging.info(f'READING CACHE {cache_path}')
                            with open(cache_path, "rb") as fh:
                                buf = io.BytesIO(fh.read())
                            return buf
                        else:
                            logging.info(f'CACHE MISS {cache_path}')

                # call the real data fetch function
                st = time.time()
                df = func(*args, **kwargs)

                # write cache if needed
                if cacheable:
                    try:
                        logging.info(f'WRITING CACHE {cache_path}')
                        if not os.path.exists(os.path.dirname(cache_path)):
                            os.makedirs(os.path.dirname(cache_path))
                        st = time.time()

                        bio = io.BytesIO()
                        df.to_pickle(bio, compression={'method': 'gzip', 'compresslevel': 1, 'mtime': 1})
                        bio.seek(0)
                        with open(cache_path, 'wb') as f:
                            f.write(bio.read())
                        bio.seek(0)
                        print(time.time() - st)
                        logging.info(f'FINISHED WRITING CACHE {cache_path}')
                        return bio
                    except Exception as e:
                        print(e)

                # success!
                return df
            except Exception as e:
                # print the traceback
                logging.error(traceback.format_exc())

                # call the real data fetch function
                df = func(*args, **kwargs)
                return df
        return new_func
    return decorator
