import os
import sqlite3
import csv
import pytest
from solution import (
    _delete_db_if_exists,
    _create_table,
    _read_and_process_ndjson,
    _extract_data_to_csv,
    main,
)


@pytest.fixture
def setup_environment(tmp_path):
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()

    ndjson_file_path = source_dir / "events.ndjson"
    ndjson_content = """
    {"event_date":"20231005","event_timestamp":"1696470452486466","event_name":"scroll","event_params":[{"key":"ga_session_id","value":{"int_value":"123526748"}},{"key":"engaged_session_event","value":{"int_value":"1"}},{"key":"session_engaged","value":{"string_value":"1"}},{"key":"page_location","value":{"string_value":"https://mobile.deexam.ph/"}},{"key":"percent_scrolled","value":{"int_value":"90"}},{"key":"ignore_referrer","value":{"string_value":"true"}},{"key":"page_title","value":{"string_value":"J's Angels"}}]}
    {"event_date":"20231005","event_timestamp":"1696470418877027","event_name":"page_view","event_params":[{"key":"page_referrer","value":{"string_value":"http://m.facebook.com/"}},{"key":"ga_session_number","value":{"int_value":"1"}},{"key":"source","value":{"string_value":"m.facebook.com"}},{"key":"entrances","value":{"int_value":"1"}},{"key":"medium","value":{"string_value":"referral"}},{"key":"session_engaged","value":{"string_value":"0"}},{"key":"page_location","value":{"string_value":"https://web.deexam.ph/"}},{"key":"campaign","value":{"string_value":"(referral)"}},{"key":"ga_session_id","value":{"int_value":"84321567"}},{"key":"page_title","value":{"string_value":"SuperJ"}},{"key":"engaged_session_event","value":{"int_value":"1"}}]}
    """
    ndjson_file_path.write_text(ndjson_content.strip())

    return {
        "ndjson_file_path": ndjson_file_path,
        "csv_file_path": target_dir / "events.csv",
        "db_file_path": tmp_path / "events.db",
    }


def test_delete_db_if_exists(setup_environment):
    paths = setup_environment

    with open(paths["db_file_path"], "w") as f:
        f.write("dummy content")

    assert os.path.exists(paths["db_file_path"])

    _delete_db_if_exists(paths["db_file_path"])

    assert not os.path.exists(paths["db_file_path"])


def test_create_table(setup_environment):
    paths = setup_environment

    conn = sqlite3.connect(paths["db_file_path"])
    cursor = conn.cursor()

    _create_table(cursor)

    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='events_table'"
    )
    table_exists = cursor.fetchone()
    conn.close()

    assert table_exists is not None


def test_read_and_process_ndjson(setup_environment):
    paths = setup_environment

    conn = sqlite3.connect(paths["db_file_path"])
    cursor = conn.cursor()

    _create_table(cursor)

    _read_and_process_ndjson(cursor, paths["ndjson_file_path"])

    conn.commit()

    cursor.execute("SELECT * FROM events_table")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 2


def test_extract_data_to_csv(setup_environment):
    paths = setup_environment

    conn = sqlite3.connect(paths["db_file_path"])
    cursor = conn.cursor()

    _create_table(cursor)

    _read_and_process_ndjson(cursor, paths["ndjson_file_path"])

    conn.commit()

    _extract_data_to_csv(cursor, paths["csv_file_path"])

    conn.close()

    assert os.path.exists(paths["csv_file_path"])

    with open(paths["csv_file_path"], newline="") as csvfile:
        csvreader = csv.reader(csvfile)
        csv_rows = list(csvreader)

    assert len(csv_rows) == 3
    assert csv_rows[0] == [
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


def test_main(setup_environment, monkeypatch):
    paths = setup_environment

    monkeypatch.setattr("solution.ndjson_file_path", paths["ndjson_file_path"])
    monkeypatch.setattr("solution.csv_file_path", paths["csv_file_path"])
    monkeypatch.setattr("solution.db_file_path", paths["db_file_path"])

    main()

    assert os.path.exists(paths["db_file_path"])

    assert os.path.exists(paths["csv_file_path"])

    conn = sqlite3.connect(paths["db_file_path"])
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM events_table")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 2

    with open(paths["csv_file_path"], newline="") as csvfile:
        csvreader = csv.reader(csvfile)
        csv_rows = list(csvreader)

    assert len(csv_rows) == 3
    assert csv_rows[0] == [
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


if __name__ == "__main__":
    pytest.main()
