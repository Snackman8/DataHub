import sqlite3
import re
import argparse
import secrets
import string

def db_connection(func):
    """Decorator to handle opening and closing the database connection."""
    def wrapper(*args, **kwargs):
        if 'db_path' not in kwargs:
            raise Exception('db_path is required to be specificed!')
        db_path = kwargs.get('db_path')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        try:
            result = func(cursor, *args, **kwargs)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
        return result
    return wrapper

@db_connection
def create_database(cursor, **kwargs):
    """Create the keys database and table if it does not exist."""
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS keys (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        allowed_path_regex TEXT NOT NULL,
        start_date_UTC TIMESTAMP,
        end_date_UTC TIMESTAMP,
        api_key TEXT NOT NULL,
        note TEXT
    )
    ''')

@db_connection
def add_key(cursor, **kwargs):
    """Add a new key to the database."""
    allowed_path_regex = input("Enter allowed path regex (default: .*) (default matches everything): ") or ".*"
    start_date = input("Enter start date (YYYY-MM-DD HH:MM:SS) or leave blank: ")
    end_date = input("Enter end date (YYYY-MM-DD HH:MM:SS) or leave blank: ")

    api_key = input("Enter API key (or press Enter to generate one): ")
    if not api_key:
        api_key = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))  # Alphanumeric, 32 characters

    note = input("Enter note: ")

    cursor.execute('''
    INSERT INTO keys (allowed_path_regex, start_date_UTC, end_date_UTC, api_key, note)
    VALUES (?, ?, ?, ?, ?)
    ''', (allowed_path_regex, start_date or None, end_date or None, api_key, note))

    print(f"\nKey added successfully.\nAPI Key: {api_key}")

@db_connection
def view_keys(cursor, **kwargs):
    """View all keys in the database."""
    cursor.execute('SELECT * FROM keys')
    rows = cursor.fetchall()

    if not rows:
        print("No keys found.")
        return

    # Print column headers
    headers = ["ID", "Allowed Path Regex", "Start Date", "End Date", "API Key", "Note"]
    header_line = f"{'ID':<5} | {'Allowed Path Regex':<30} | {'Start Date':<20} | {'End Date':<20} | {'API Key':<34} | {'Note':<20}"
    print(header_line)
    print("-" * len(header_line))

    # Print rows in a fixed-width format, converting None to an empty string
    for row in rows:
        formatted_row = [str(col) if col is not None else '' for col in row]
        print(f"{formatted_row[0]:<5} | {formatted_row[1]:<30} | {formatted_row[2]:<20} | {formatted_row[3]:<20} | {formatted_row[4]:<34} | {formatted_row[5]:<20}")

@db_connection
def update_key(cursor, **kwargs):
    """Update an existing key."""
    key_id = input("Enter the ID of the key to update: ")

    cursor.execute('SELECT * FROM keys WHERE id = ?', (key_id,))
    key = cursor.fetchone()

    if not key:
        print("Key not found.")
        return

    allowed_path_regex = input(f"Enter new allowed path regex (current: {key[1]}): ") or key[1]
    start_date = input(f"Enter new start date (current: {key[2]}) or leave blank: ") or key[2]
    end_date = input(f"Enter new end date (current: {key[3]}) or leave blank: ") or key[3]
    api_key = input(f"Enter new API key (current: {key[4]}): ") or key[4]
    note = input(f"Enter new note (current: {key[5]}): ") or key[5]

    cursor.execute('''
    UPDATE keys
    SET allowed_path_regex = ?, start_date_UTC = ?, end_date_UTC = ?, api_key = ?, note = ?
    WHERE id = ?
    ''', (allowed_path_regex, start_date, end_date, api_key, note, key_id))

    print("Key updated successfully.")

@db_connection
def delete_key(cursor, **kwargs):
    """Delete a key from the database."""
    key_id = input("Enter the ID of the key to delete: ")

    cursor.execute('DELETE FROM keys WHERE id = ?', (key_id,))

    print("Key deleted successfully.")

def main(args):
    """Main console application loop."""
    create_database(db_path=args.db_path)

    while True:
        print("\nKey Management Console")
        print("1. Add Key")
        print("2. View Keys")
        print("3. Update Key")
        print("4. Delete Key")
        print("5. Exit")

        choice = input("Enter your choice: ")

        if choice == '1':
            add_key(db_path=args.db_path)
        elif choice == '2':
            view_keys(db_path=args.db_path)
        elif choice == '3':
            update_key(db_path=args.db_path)
        elif choice == '4':
            delete_key(db_path=args.db_path)
        elif choice == '5':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")


def console_entry():
    # parse command line arguments
    parser = argparse.ArgumentParser(description="Key Management Console")
    parser.add_argument('--db_path', type=str, help='Path to the SQLite database file, i.e. /tmp/keys.db', required=True)
    args = parser.parse_args()

    # run the main
    main(args)


if __name__ == "__main__":
    # parse command line arguments
    console_entry()
