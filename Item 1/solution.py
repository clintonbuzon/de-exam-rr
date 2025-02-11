import json
import sqlite3
import csv
import os
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

ndjson_file_path = os.path.join(os.path.dirname(__file__), "source/events.ndjson")
csv_file_path = os.path.join(os.path.dirname(__file__), "target/events.csv")
db_file_path = "events.db"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS events_table (
    event_date TEXT,
    event_timestamp TEXT,
    event_name TEXT,
    ga_session_id INTEGER,
    engaged_session_event INTEGER,
    page_location TEXT,
    page_title TEXT,
    page_referrer TEXT,
    source TEXT
)
"""

INSERT_EVENT_SQL = """
INSERT INTO events_table 
(
    event_date, 
    event_timestamp, 
    event_name, 
    ga_session_id, 
    engaged_session_event, 
    page_location, 
    page_title, 
    page_referrer, 
    source
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

SELECT_ALL_EVENTS_SQL = "SELECT * FROM events_table"


def _delete_db_if_exists(db_path):
    if os.path.exists(db_path):
        os.remove(db_path)
        logging.info(f"Deleted existing database file: {db_path}")


def _create_table(cursor):
    cursor.execute(CREATE_TABLE_SQL)
    logging.info("Created events_table")


def _insert_event(cursor, event):
    event_date = event.get("event_date")
    event_timestamp = event.get("event_timestamp")
    event_name = event.get("event_name")
    event_params = {
        param["key"]: param["value"].get("string_value")
        or param["value"].get("int_value")
        for param in event.get("event_params", [])
    }
    cursor.execute(
        INSERT_EVENT_SQL,
        (
            event_date,
            event_timestamp,
            event_name,
            event_params.get("ga_session_id"),
            event_params.get("engaged_session_event"),
            event_params.get("page_location"),
            event_params.get("page_title"),
            event_params.get("page_referrer"),
            event_params.get("source"),
        ),
    )
    logging.info(f"Inserted event: {event_name} at {event_timestamp}")


def _read_and_process_ndjson(cursor, file_path):
    with open(file_path, "r") as file:
        for line in file:
            event = json.loads(line)
            _insert_event(cursor, event)
    logging.info(f"Processed NDJSON file: {file_path}")


def _extract_data_to_csv(cursor, csv_path):
    cursor.execute(SELECT_ALL_EVENTS_SQL)
    rows = cursor.fetchall()
    with open(csv_path, "w", newline="") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(
            [
                "event_date",
                "event_timestamp",
                "event_name",
                "ga_session_id",
                "engaged_session_event",
                "page_location",
                "page_title",
                "page_referrer",
                "source",
            ]
        )
        csvwriter.writerows(rows)
    logging.info(f"Extracted data to CSV file: {csv_path}")


def main():
    logging.info("Starting the process")

    _delete_db_if_exists(db_file_path)

    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    _create_table(cursor)

    _read_and_process_ndjson(cursor, ndjson_file_path)

    conn.commit()
    logging.info("Committed the transaction")

    _extract_data_to_csv(cursor, csv_file_path)

    conn.close()
    logging.info("Closed the database connection")

    logging.info("Process completed successfully")


if __name__ == "__main__":
    main()
