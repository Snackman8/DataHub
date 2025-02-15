""" DataHub Web Application """
# --------------------------------------------------
#    Imports
# --------------------------------------------------
import argparse
import json
import logging
import os
import sqlite3
import sys
import traceback
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response, FileResponse, JSONResponse
from contextlib import asynccontextmanager
import uvicorn
try:
    from DataHub.openapi_schema_generator import generate_openapi_schema
    import DataHub.business_logic as business_logic
except:
    from openapi_schema_generator import generate_openapi_schema
    import business_logic


# --------------------------------------------------
#    Functions
# --------------------------------------------------
def _str_to_bool(value):
    """
    Convert a string to a boolean. Recognizes "true", "yes", "1" as True and
    "false", "no", "0" as False (case-insensitive). Raises ValueError for invalid input.
    """
    truthy = {"true", "t", "yes", "y", "1"}
    falsy = {"false", "f", "no", "n", "0"}
    value = value.strip().lower()
    if value in truthy:
        return True
    elif value in falsy:
        return False
    raise ValueError(f"Invalid boolean value: {value}")


def _validate_api_key(api_key: str):
    """
    Validates the provided api_key against the database.
    Also checks if the current date is within the valid start_date and end_date range.
    """
    if not api_key:
        raise HTTPException(status_code=400, detail="Missing API Key")

    try:
        # Connect to the database
        conn = sqlite3.connect(app.state.db_path)  # Replace with your database path
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        cursor = conn.cursor()

        # Query to validate the key and date range
        cursor.execute("""
            SELECT * FROM keys
            WHERE api_key = ?
              AND (start_date_UTC IS NULL OR start_date_UTC <= date('now'))
              AND (end_date_UTC IS NULL OR end_date_UTC >= date('now'))
        """, (api_key,))

        key_info = cursor.fetchone()
        conn.close()

        if not key_info:
            raise HTTPException(status_code=401, detail="Invalid or expired API key")

        return key_info  # Return validated key information if needed
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# --------------------------------------------------
#    Handlers
# --------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info(f'cwd = {os.getcwd()}')
    logging.info(f'module_path = {os.getenv("MODULE_PATH", "missing")}')
    app.state.module_path = os.path.abspath(os.getenv("MODULE_PATH", "missing"))
    logging.info(f'absolute module_path = {app.state.module_path}')
    app.state.disable_auth = _str_to_bool(os.getenv("DISABLE_AUTH", 'False'))
    app.state.disable_auth_on_static_files = _str_to_bool(os.getenv("DISABLE_AUTH_ON_STATIC_FILES", 'False'))
    app.state.db_path = os.getenv('DB_PATH', '')
    app.state.allow_url_auth = _str_to_bool(os.getenv("ALLOW_URL_AUTH", 'False'))
    app.state.static_path = os.getenv("STATIC_PATH", None)

    # modify the sys path if needed
    if app.state.module_path != '.':
        if app.state.module_path not in sys.path:
            sys.path.append(app.state.module_path)
    yield


app = FastAPI(lifespan=lifespan)


@app.api_route("/{full_path:path}", methods=["GET", "POST"])
async def handle_all_requests(full_path: str, request: Request=None):
    """
    Handles all incoming requests dynamically:
    - Validates API keys passed as query parameters.
    - Serves static files if the path is a file.
    - Executes business logic for non-file requests.
    """
    # Parse query parameters
    parsed_qs = dict(request.query_params)

    # If POST, merge with JSON body
    if request.method == "POST":
        try:
            body = await request.json()
            parsed_qs.update(body)  # Merge JSON body into params
        except Exception:
            pass  # Ignore if no JSON body is present

    qid = parsed_qs.get("qid", "")
    nospawn = parsed_qs.pop("nospawn", [""])[0]
    api_key = parsed_qs.pop("api-key", None)

    # apache proxy may prepend the full path with a /, remove it
    if full_path.startswith('/'):
        full_path = full_path[1:]

    # don't do authentication on static path files
    authenticate = True
    if app.state.disable_auth:
        authenticate = False
    if app.state.disable_auth_on_static_files:
        if os.path.isfile(os.path.join(app.state.static_path, full_path)):
            authenticate = False
    if authenticate:
        if not app.state.allow_url_auth:
            if api_key:
                raise HTTPException(status_code=400, detail='API-Key is not allowed to be passed in through the URL unless --allow_url_auth is set')

        # Check headers
        api_key = request.headers.get("API-Key", api_key)

        # check if passed in using x-api-key
        if 'x-api-key' in request.headers:
            api_key = request.headers['x-api-key']

        # Validate API keys
        _validate_api_key(api_key)

    try:
        if qid == "":
            # Serve static files if possible
            if (bool(os.path.splitext(full_path)[1])):
                if os.path.isfile(os.path.join(app.state.static_path, full_path)):
                    return FileResponse(os.path.join(app.state.static_path, full_path))
                else:
                    return JSONResponse(status_code=404,content={"message": "Does not exist"})

            # check if this can be handled as a query
            try:
                # Execute query logic as if last part of path was a qid
                if '/' in full_path:
                    tmp_full_path, qid = full_path.rsplit('/', 1)
                    tmp_parsed_qs = dict(parsed_qs)
                    tmp_parsed_qs['qid'] = qid
                    html, content_type, return_code, headers = await business_logic.execute_query(
                        tmp_full_path, tmp_parsed_qs, nospawn not in ["", "0"]
                    )
                    return Response(content=html, media_type=content_type, status_code=return_code, headers=headers)
            except Exception as e:
                print(e)
                pass

            # Generate and return HTML documentation
            host = request.headers.get("host", "")

            # special case of apache proxy
            if 'x-request-uri' in request.headers:
                if request.headers.get('x-request-uri').endswith(full_path):
                    host = request.headers.get('x-request-uri')
                    if full_path != '':
                        host = request.headers.get('x-request-uri')[:-len(full_path)].strip('/')

            html, content_type, return_code = business_logic.build_html_docs(
                host=host,
                path=full_path,
                module_path=app.state.module_path
            )
            return Response(content=html, media_type="text/html", status_code=200)
        else:
            # Execute query logic
            html, content_type, return_code, headers = business_logic.execute_query(
                full_path, parsed_qs, nospawn not in ["", "0"]
            )
            return Response(content=html, media_type=content_type, status_code=return_code, headers=headers)
    except:
        html = f"<pre>{traceback.format_exc()}</pre>"
        return Response(content=html, media_type="text/html", status_code=500)


# --------------------------------------------------
#    Main
# --------------------------------------------------
def main(args):
    os.environ["MODULE_PATH"] = str(args['module_path'])
    os.environ["DISABLE_AUTH"] = str(args['disable_auth'])
    os.environ["DISABLE_AUTH_ON_STATIC_FILES"] = str(args['disable_auth_on_static_files'])
    os.environ["DB_PATH"] = str(args['db_path'])
    os.environ["ALLOW_URL_AUTH"] = str(args['allow_url_auth'])
    if args['static_path']:
        os.environ["STATIC_PATH"] = str(args['static_path'])
    else:
        os.environ["STATIC_PATH"] = os.path.join(str(args['module_path']), '_static')

    if args['generate_schema']:
        schema = generate_openapi_schema(args['module_path'], args['openapi_server_url'])
        print(schema)
        return

    logging.info(f'\nTo manage API keys, run datahub_api_key_manager --db_path {args["db_path"]}\n')

    try:
        uvicorn.run("DataHub.dataHub:app", host=args['host'], port=args['port'], reload=False, log_level=args['loglevel'].lower())
    except:
        uvicorn.run("dataHub:app", host=args['host'], port=args['port'], reload=False, log_level=args['loglevel'].lower())


def console_entry():
    # parse command line arguments
    default_provider_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'example_provider/src')
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", help="host to serve webapp on, i.e. 0.0.0.0 or 127.0.0.1", default='0.0.0.0', required=False)
    parser.add_argument("--port", type=int, help="port to serve webapp on", default=9151, required=False)
    parser.add_argument("--loglevel", help="logging level, i.e. INFO", default='INFO', required=False)
    parser.add_argument("--module_path", help="location of additional modules", default=default_provider_path, required=False)
    parser.add_argument("--disable_auth", help="Disable authentication for testing (default: False)", action="store_true")
    parser.add_argument("--disable_auth_on_static_files", help="Disable authentication only on static files (default: False)", action="store_true")
    parser.add_argument("--allow_url_auth", help="Allow authentication by passing in access key and secret key in URL (insecure)", action="store_true")
    parser.add_argument("--db_path", help="Path to the api keys database file (default: keys.db)", type=str, default="keys.db")
    parser.add_argument("--generate_schema", help="generate the OpenAPI Schema", action="store_true")
    parser.add_argument("--openapi_server_url", help="OpenAPI Schema server url", type=str, default="http://localhost")
    parser.add_argument("--static_path", help="path to static files, default is _static directory under the module path", type=str, default=None)
    args = parser.parse_args()
    args = vars(args)

    # start the thread and the app
    logging.basicConfig(level=args['loglevel'].upper(), format='%(asctime)s %(relativeCreated)6d %(threadName)s %(message)s')

    # run the main
    main(args)


if __name__ == "__main__":
    # parse command line arguments
    console_entry()
