import os
import sqlite3
import pandas as pd
import pytest
from solution import _delete_db_if_exists, _load_data, _create_views, main


@pytest.fixture
def setup_environment(tmp_path):
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()

    date_csv_path = source_dir / "date.csv"
    date_csv_content = """DATE_KEY,DATE_FLD
1,2023-01-01
2,2023-01-02
3,2023-01-03
4,2023-01-04
5,2023-01-05
6,2023-01-06
7,2023-01-07
8,2023-01-08
9,2023-01-09
10,2023-01-10
11,2023-01-11
12,2023-01-12
13,2023-01-13
14,2023-01-14
15,2023-01-15
16,2023-01-16
17,2023-01-17
18,2023-01-18
19,2023-01-19
20,2023-01-20
21,2023-01-21
22,2023-01-22
23,2023-01-23
24,2023-01-24
25,2023-01-25
26,2023-01-26
27,2023-01-27
28,2023-01-28
29,2023-01-29
30,2023-01-30
31,2023-01-31
32,2023-02-01
33,2023-02-02
34,2023-02-03
35,2023-02-04
36,2023-02-05
37,2023-02-06
38,2023-02-07
39,2023-02-08
40,2023-02-09
41,2023-02-10
42,2023-02-11
43,2023-02-12
44,2023-02-13
45,2023-02-14
"""
    date_csv_path.write_text(date_csv_content.strip())

    stores_csv_path = source_dir / "stores.csv"
    stores_csv_content = """STORE_KEY,STORE_CODE,STORE_DESCRIPTION
1,001,Store 1
2,002,Store 2
3,003,Store 3
"""
    stores_csv_path.write_text(stores_csv_content.strip())

    fsl_csv_path = source_dir / "fsl.csv"
    fsl_csv_content = """STORE_KEY,DATE_KEY,SALE_NET_VAL
1,1,100
1,2,150
1,3,200
2,1,300
2,2,350
2,3,400
3,1,500
3,2,550
3,3,600
"""
    fsl_csv_path.write_text(fsl_csv_content.strip())

    return {
        "date_csv_path": date_csv_path,
        "stores_csv_path": stores_csv_path,
        "fsl_csv_path": fsl_csv_path,
        "db_file_path": tmp_path / "sales.db",
    }


def test_delete_db_if_exists(setup_environment):
    paths = setup_environment

    with open(paths["db_file_path"], "w") as f:
        f.write("dummy content")

    assert os.path.exists(paths["db_file_path"])

    _delete_db_if_exists(paths["db_file_path"])

    assert not os.path.exists(paths["db_file_path"])


def test_load_data(setup_environment, monkeypatch):
    paths = setup_environment

    monkeypatch.setattr("solution.date_csv_path", paths["date_csv_path"])
    monkeypatch.setattr("solution.stores_csv_path", paths["stores_csv_path"])
    monkeypatch.setattr("solution.fsl_csv_path", paths["fsl_csv_path"])
    monkeypatch.setattr("solution.db_file_path", paths["db_file_path"])

    _load_data()

    conn = sqlite3.connect(paths["db_file_path"])
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM date_table")
    date_rows = cursor.fetchall()
    assert len(date_rows) == 45

    cursor.execute("SELECT * FROM stores_table")
    stores_rows = cursor.fetchall()
    assert len(stores_rows) == 3

    cursor.execute("SELECT * FROM fsl_table")
    fsl_rows = cursor.fetchall()
    assert len(fsl_rows) == 9

    conn.close()


def test_create_views(setup_environment, monkeypatch):
    paths = setup_environment

    monkeypatch.setattr("solution.date_csv_path", paths["date_csv_path"])
    monkeypatch.setattr("solution.stores_csv_path", paths["stores_csv_path"])
    monkeypatch.setattr("solution.fsl_csv_path", paths["fsl_csv_path"])
    monkeypatch.setattr("solution.db_file_path", paths["db_file_path"])

    _load_data()

    _create_views()

    conn = sqlite3.connect(paths["db_file_path"])
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM store_sales_view")
    view_rows = cursor.fetchall()
    assert len(view_rows) == 3

    conn.close()


def test_main(setup_environment, monkeypatch):
    paths = setup_environment

    monkeypatch.setattr("solution.date_csv_path", paths["date_csv_path"])
    monkeypatch.setattr("solution.stores_csv_path", paths["stores_csv_path"])
    monkeypatch.setattr("solution.fsl_csv_path", paths["fsl_csv_path"])
    monkeypatch.setattr("solution.db_file_path", paths["db_file_path"])

    main()

    conn = sqlite3.connect(paths["db_file_path"])
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM date_table")
    date_rows = cursor.fetchall()
    assert len(date_rows) == 45

    cursor.execute("SELECT * FROM stores_table")
    stores_rows = cursor.fetchall()
    assert len(stores_rows) == 3

    cursor.execute("SELECT * FROM fsl_table")
    fsl_rows = cursor.fetchall()
    assert len(fsl_rows) == 9

    cursor.execute("SELECT * FROM store_sales_view")
    view_rows = cursor.fetchall()
    assert len(view_rows) == 3

    conn.close()


if __name__ == "__main__":
    pytest.main()
