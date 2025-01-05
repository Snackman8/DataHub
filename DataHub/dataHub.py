""" DataHub Web Application """
# --------------------------------------------------
#    Imports
# --------------------------------------------------
import argparse
import logging
import os
import sqlite3
import sys
import traceback
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response, FileResponse
import uvicorn
import business_logic


# --------------------------------------------------
#    Globals
# --------------------------------------------------
app = FastAPI()


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


def _validate_api_key(access_key: str, secret_key: str):
    """
    Validates the provided access_key and secret_key against the database.
    Also checks if the current date is within the valid start_date and end_date range.
    """
    if not access_key or not secret_key:
        raise HTTPException(status_code=400, detail="Missing Access-Key or Secret-Key")

    try:
        # Connect to the database
        conn = sqlite3.connect(app.state.db_path)  # Replace with your database path
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        cursor = conn.cursor()

        # Query to validate the keys and date range
        cursor.execute("""
            SELECT * FROM keys
            WHERE access_key = ? AND secret_key = ?
              AND status = 'Active'
              AND (start_date_UTC IS NULL OR start_date_UTC <= date('now'))
              AND (end_date_UTC IS NULL OR end_date_UTC >= date('now'))
        """, (access_key, secret_key))

        key_info = cursor.fetchone()
        conn.close()

        if not key_info:
            raise HTTPException(status_code=401, detail="Invalid or expired API keys")

        return key_info  # Return validated key information if needed
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# --------------------------------------------------
#    Handlers
# --------------------------------------------------
@app.on_event("startup")
async def startup_event():
    app.state.module_path = os.getenv("MODULE_PATH", "missing")
    app.state.disable_auth = _str_to_bool(os.getenv("DISABLE_AUTH", 'False'))
    app.state.db_path = os.getenv('DB_PATH', '')
    app.state.allow_url_auth = _str_to_bool(os.getenv("ALLOW_URL_AUTH", 'False'))

    # modify the sys path if needed
    if app.state.module_path != '.':
        if app.state.module_path not in sys.path:
            sys.path.append(app.state.module_path)


@app.get("/{full_path:path}")
def handle_all_requests(full_path: str, request: Request=None):
    """
    Handles all incoming requests dynamically:
    - Validates API keys passed as query parameters.
    - Serves static files if the path is a file.
    - Executes business logic for non-file requests.
    """
    # Parse query parameters
    parsed_qs = dict(request.query_params)
    qid = parsed_qs.get("qid", "")
    nospawn = parsed_qs.pop("nospawn", [""])[0]
    access_key = parsed_qs.pop("Access-Key", None)
    secret_key = parsed_qs.pop("Secret-Key", None)

    if not app.state.disable_auth:
        if not app.state.allow_url_auth:
            if access_key or secret_key:
                raise HTTPException(status_code=400, detail='Access-Key and Secret-Key are not allowed to be passed in through the URL unless --allow_url_auth is set')

        # Check headers
        access_key = request.headers.get("Access-Key", access_key)
        secret_key = request.headers.get("Secret-Key", secret_key)

        # Validate API keys
        _validate_api_key(access_key, secret_key)

    # Serve static files
    if os.path.isfile(full_path):
        return FileResponse(full_path)


    try:
        if qid == "":
            # Generate and return HTML documentation
            html, content_type, return_code = business_logic.build_html_docs(
                host=request.headers.get("host", ""),
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
    os.environ["MODULE_PATH"] = args['module_path']
    os.environ["DISABLE_AUTH"] = str(args['disable_auth'])
    os.environ["DB_PATH"] = str(args['db_path'])
    os.environ["ALLOW_URL_AUTH"] = str(args['allow_url_auth'])
    uvicorn.run("dataHub:app", host="0.0.0.0", port=args['port'], reload=False)


if __name__ == "__main__":
    # parse command line arguments
    default_provider_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'example_providers')
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, help="port to serve webapp on", default=9151, required=False)
    parser.add_argument("--loglevel", help="logging level, i.e. INFO", default='INFO', required=False)
    parser.add_argument("--module_path", help="location of additional modules", default=default_provider_path, required=False)
    parser.add_argument("--disable_auth", help="Disable authentication for testing (default: False)", action="store_true", default='False')
    parser.add_argument("--allow_url_auth", help="Allow authentication by passing in access key and secret key in URL (insecure)", action="store_true", default='False')
    parser.add_argument("--db_path", help="Path to the SQLite database file (default: my_database.db)", type=str, default="keys.db")
    args = parser.parse_args()
    args = vars(args)

    # start the thread and the app
    logging.basicConfig(level=args['loglevel'].upper(), format='%(asctime)s %(relativeCreated)6d %(threadName)s %(message)s')

    # run the main
    main(args)
