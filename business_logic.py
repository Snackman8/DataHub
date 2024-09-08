""" business logic for DataHub"""

# --------------------------------------------------
#    Imports
# --------------------------------------------------
import concurrent.futures
import importlib
import inspect
import io
import os
import pretty_html_table
import pandas as pd
from CCDILib.utils.dateutils import normalize_start_date_end_date


# --------------------------------------------------
#    Functions
# --------------------------------------------------
def _build_html_docs_worker(host, path, modulepath, authuser, authtoken, callerid):
    """ worker to build html docs for a path """

    # add style sheet
    html = '<head><link rel="stylesheet" href="/style.css"></head>'
    html = html + f'<h1><a href="/">DataHub Information</a></h1><pre class=path>Path: /{path}</pre><hr>'

    # build the authentication tail
    tail = ''
    if authuser != '':
        tail = tail + f'&authuser={authuser}'
    if authtoken != '':
        tail = tail + f'&authtoken={authtoken}'
    if callerid != '':
        tail = tail + f'&callerid={callerid}'

    # check if we are loading a python file
    if not os.path.exists(os.path.join(modulepath, path) + '.py'):
        # does not exist, so build a directory listing
        dir_list =sorted(os.listdir(os.path.join(modulepath, path)))

        for x in dir_list:
            if not x.startswith('.') and not x.startswith('_'):
                try:
                    if x.endswith('.py'):
                        x = x[:-3]
                        m = importlib.import_module(os.path.join(path, x).replace('/', '.'))
                    else:
                        m = importlib.import_module(os.path.join(path, x))
                    html = html + f'<div class=divmodule><h4>/{os.path.join(path, x)}</h4>'
                    if m.__doc__:
                        html = html + f'<pre class=doc>{m.__doc__}</pre>'
                    aref = 'http://' + os.path.join(host, path, x) + (('?' + tail[1:]) if tail else '')
                    html = html + f'<a class=a href={aref}>{aref}</a>'
                    html = html + f'<hr></div>'
                except:
                    pass

        return html, 'text/html', 200

    # try to load the functions in the module
    m = importlib.import_module(path.replace('/', '.'))
    funcs = []
    for name in dir(m):
        if not name.startswith('_'):
            try:
                if getattr(m, name).__module__ == m.__name__:
                    if inspect.isfunction(getattr(m, name)):
                        funcs.append(name)
            except:
                # don't include if there are problems
                pass

    # build the html doc string
    for name in sorted(funcs):
        docs = getattr(m, name).__doc__
        if docs is None:
            html = f'<div style="color: red; height:200px; background-color: pink; font-size: 40px;">Error!  No documentation found for {name}.<br>This is not OK</div>' + html
        else:
            doc_lines = []
            for x in docs.split('\n'):
                if 'Example Query: ' in x:
                    aref = x.partition('Example Query:')[2].strip()
                    aref = f"http://{host}/{path}?qid={name}{aref}{tail}"
                    x = f'<a href={aref}>{aref}</a>'
                doc_lines.append(x)
            docs = '\n'.join(doc_lines)

            html = html + f'<div class=divmodule><h4>/{path + "?qid=" + name}</h4>'
            html = html + f'<pre class=doc>{docs}</pre></div><hr>'
    return html, 'text/html', 200


def build_html_docs(host, path, modulepath, authuser, authtoken, callerid):
    """ build html docs for a path

        Args:
            host - host the server is running on, i.e. ccgtdev.commercecasino.local
            path - path to inspect to build documentation for
            modulepath - system path to the modules

        Returns:
            html docs
    """
    with concurrent.futures.ProcessPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_build_html_docs_worker, host, path, modulepath, authuser, authtoken, callerid)
        result = future.result()
        return result


def _execute_query_worker(path, parsed_qs):
    """ worker to execute a data query """
    # load the functions in the module
    m = importlib.import_module(path.replace('/', '.'))
    qid = parsed_qs.pop('qid')
    result = getattr(m, qid)(**parsed_qs)

    # check if this is already a pickle
    if not isinstance(result, io.BytesIO):
        f = io.BytesIO()
        result.to_pickle(f, compression='gzip')
        f.seek(0)
        result = f.read()
        f.close()
    else:
        result.seek(0)
        result = result.read()
    return result


def _execute_fast_cache_worker(path, parsed_qs):
    """ worker to execute a data query if asking for fast_cache"""
    try:
        # load the functions in the module
        m = importlib.import_module(path.replace('/', '.'))
        cache_dir = getattr(m, 'CACHE_ROOT_DIR')

        # get the normalized start_date and end_date
        start_date, end_date, _, _ = normalize_start_date_end_date(parsed_qs['start_date'], parsed_qs['end_date'])
        subpath = os.path.split(path)[1]
        func_name = parsed_qs['qid']
        cache_filename = func_name + '?start_date=' + start_date.strftime('%Y-%m-%d %H:%M:%S') + '&end_date=' + end_date.strftime('%Y-%m-%d %H:%M:%S') + ".pickle.gz"
        cache_path = os.path.join(cache_dir, subpath, cache_filename).replace(' ', '_').replace('&', '_').replace('?', '_')

        return 'fast_cache://' + cache_path
    except:
        return ''


def execute_query(path, parsed_qs, nospawn=False):
    """ execute a data query and return the results

        Args:
            path - path to module to execute
            parsed_qs - dictionary of query parameters
            nospawn - if set to True, do not spawn a separate process

        Return:
            data
    """
    with concurrent.futures.ProcessPoolExecutor(max_workers=1) as executor:
        # get the output format
        output = parsed_qs.pop('output', 'csv')

        # special for fast_cache
        if output == 'fast_cache':
            # try to return the path to a cache file directly
            if not nospawn:
                future = executor.submit(_execute_fast_cache_worker, path, parsed_qs)
                fast_cache_path = future.result()
            else:
                fast_cache_path = _execute_fast_cache_worker(path, parsed_qs)

            content_type = 'application/fast_cache'
            return fast_cache_path, content_type, 200

        # handle all other formats
        if not nospawn:
            future = executor.submit(_execute_query_worker, path, parsed_qs)
            result = future.result()
        else:
            result = _execute_query_worker(path, parsed_qs)

        f = io.BytesIO(result)

        headers = {}
        content_type = 'text/plain'
        if output == 'csv':
            result = pd.read_pickle(f, compression='gzip').to_csv()
        elif output == 'json':
            content_type = 'application/json'
            result = pd.read_pickle(f, compression='gzip').to_json()
        elif output == 'pickle':
            content_type = 'application/python-pickle'
            headers['Content-Encoding'] = 'gzip'
        elif output == 'html':
            # convert the dataframe to pretty html
            result = pd.read_pickle(f, compression='gzip')
            result = pretty_html_table.build_table(result.reset_index(), 'blue_light')
            content_type = 'text/html'
        elif output == 'fast_cache':
            pass

        f.close()
        return result, content_type, 200, headers
