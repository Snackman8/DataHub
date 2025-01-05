# --------------------------------------------------
#    Imports
# --------------------------------------------------
import json
import logging
import random
import string
import sqlite3
from pylinkjs.PyLinkJS import run_pylinkjs_app


# --------------------------------------------------
#    Constants
# --------------------------------------------------
DB_FILE = '../../DataHub/keys.db'


# --------------------------------------------------
#    Functions
# --------------------------------------------------
def _db_delete_key(access_key):
    """
    Delete a key from the database.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM keys WHERE access_key = ?", (access_key,))
    conn.commit()
    conn.close()


def _db_get_keys():
    """
    Fetch all keys from the database.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT key_group, start_date_UTC, end_date_UTC, note, access_key, secret_key, status FROM keys')
    keys = [
        {
            "key_group": row[0],
            "start_date_UTC": row[1],
            "end_date_UTC": row[2],
            "note": row[3],
            "access_key": row[4],
            "secret_key": row[5],
            "status": row[6],
        }
        for row in cursor.fetchall()
    ]
    conn.close()
    return keys


def _db_init():
    """
    Initialize the SQLite database and create the keys table if it does not exist.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key_group TEXT NOT NULL DEFAULT 'Default',  -- Ensure group is required and has a default
            start_date_UTC TIMESTAMP,
            end_date_UTC TIMESTAMP,
            note TEXT,
            access_key TEXT UNIQUE,
            secret_key TEXT,
            status TEXT
        )
    """)
    conn.commit()
    conn.close()


def _db_save_key(key):
    """
    Insert or update a key in the database, ensuring `key_group` defaults to 'Default'.
    """
    key['key_group'] = key.get('key_group', 'Default') or 'Default'  # Ensure a default value
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO keys (key_group, start_date_UTC, end_date_UTC, note, access_key, secret_key, status)
        VALUES (:key_group, :start_date_UTC, :end_date_UTC, :note, :access_key, :secret_key, :status)
        ON CONFLICT(access_key) DO UPDATE SET
            key_group=excluded.key_group,
            start_date_UTC=excluded.start_date_UTC,
            end_date_UTC=excluded.end_date_UTC,
            note=excluded.note,
            secret_key=excluded.secret_key,
            status=excluded.status
    """, key)
    conn.commit()
    conn.close()


def _db_toggle_key_status(access_key):
    """
    Toggle the status of a key in the database.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE keys
        SET status = CASE WHEN status = 'Active' THEN 'Suspended' ELSE 'Active' END
        WHERE access_key = ?
    """, (access_key,))
    conn.commit()
    conn.close()


# --------------------------------------------------
#    Event Handlers
# --------------------------------------------------
def ready(jsc, origin, pathname, search):
    """
    Send the existing keys to the frontend when the page loads.
    """
    for key in _db_get_keys():
        jsc.eval_js_code(f"createOrUpdateTableRow({json.dumps(key)})")


def delete_key(jsc, access_key):
    """
    Delete a key from the database based on the access_key and update the table.
    """
    _db_delete_key(access_key)
    jsc.eval_js_code(f"deleteTableRow('{access_key}')")


def generate_key(jsc, field_id):
    """
    Generate a new key and update the corresponding field in the frontend.
    """
    new_key = ''.join(random.choices(string.ascii_letters + string.digits, k=20))
    jsc[f'#{field_id}'].val = new_key


def get_keys(jsc):
    """
    Send the list of keys as JSON to the frontend.
    """
    keys = _db_get_keys()
    jsc.eval_js_code(f"updateTableRows({json.dumps(keys)})")


def save_key(jsc, key_group, start_date_UTC, end_date_UTC, note, access_key, secret_key):
    """
    Add a new API key or update an existing key, ensuring `group` defaults to 'Default'.
    """
    # Convert empty strings to None (equivalent to NULL in SQLite)
    start_date_UTC = None if start_date_UTC == '' else start_date_UTC
    end_date_UTC = None if end_date_UTC == '' else end_date_UTC

    existing_key = next((k for k in _db_get_keys() if k['access_key'] == access_key), None)

    updated_key = {
        "key_group": key_group,
        "start_date_UTC": start_date_UTC,
        "end_date_UTC": end_date_UTC,
        "note": note,
        "access_key": access_key,
        "secret_key": secret_key,
        "status": existing_key['status'] if existing_key else "Active",  # Preserve status or default to Active
    }

    _db_save_key(updated_key)
    jsc.eval_js_code(f"createOrUpdateTableRow({json.dumps(updated_key)})")


def toggle_suspend_key(jsc, access_key):
    """
    Toggle the suspension status of an API key.
    """
    _db_toggle_key_status(access_key)
    updated_key = next((k for k in _db_get_keys() if k['access_key'] == access_key), None)
    if updated_key:
        jsc.eval_js_code(f"createOrUpdateTableRow({json.dumps(updated_key)})")


# --------------------------------------------------
#    Main
# --------------------------------------------------
def main():
    """
    Main entry point for the application.
    Initializes the database and starts the PyLinkJS application.
    """
    _db_init()
    logging.basicConfig(level=logging.DEBUG, format='%(relativeCreated)6d %(threadName)s %(message)s')
    run_pylinkjs_app(default_html='APIKeyDashboard.html', port=8501, ready=ready)

if __name__ == "__main__":
    main()
