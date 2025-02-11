import sqlite3
import pandas as pd
import os
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

base_path = os.path.dirname(__file__)
date_csv_path = os.path.join(base_path, "date.csv")
stores_csv_path = os.path.join(base_path, "stores.csv")
fsl_csv_path = os.path.join(base_path, "fsl.csv")
db_file_path = os.path.join(base_path, "sales.db")

# Assuming the current date is 2023-02-14 to provide a reference for the date calculations
CURRENT_DATE = "2023-02-14"

CREATE_VIEW_SQL = f"""
CREATE VIEW store_sales_view AS
SELECT
    s.STORE_KEY,
    s.STORE_CODE,
    s.STORE_DESCRIPTION,
    -- Year-to-date sales
    SUM(CASE WHEN d.DATE_FLD BETWEEN strftime('%Y-01-01', '{CURRENT_DATE}') AND '{CURRENT_DATE}' THEN f.SALE_NET_VAL ELSE 0 END) AS YTD_SALES,
    -- Month-to-date sales
    SUM(CASE WHEN strftime('%Y-%m', d.DATE_FLD) = strftime('%Y-%m', '{CURRENT_DATE}') THEN f.SALE_NET_VAL ELSE 0 END) AS MTD_SALES,
    -- Week-to-date sales (Monday to Sunday)
    SUM(CASE WHEN d.DATE_FLD BETWEEN date('{CURRENT_DATE}', 'weekday 1', '-7 days') AND '{CURRENT_DATE}' THEN f.SALE_NET_VAL ELSE 0 END) AS WTD_SALES
FROM
    stores_table s
JOIN
    fsl_table f ON s.STORE_KEY = f.STORE_KEY
JOIN
    date_table d ON f.DATE_KEY = d.DATE_KEY
WHERE
    strftime('%Y', d.DATE_FLD) = '2023'
GROUP BY
    s.STORE_KEY, s.STORE_CODE, s.STORE_DESCRIPTION;
"""


def _delete_db_if_exists(db_file_path):
    if os.path.exists(db_file_path):
        os.remove(db_file_path)
        logging.info(f"Deleted existing database file: {db_file_path}")


def _load_data():
    logging.info("Loading data into SQLite database")

    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    date_df = pd.read_csv(date_csv_path)
    date_df.to_sql("date_table", conn, if_exists="replace", index=False)
    logging.info("Loaded date.csv into date_table")

    stores_df = pd.read_csv(stores_csv_path)
    stores_df.to_sql("stores_table", conn, if_exists="replace", index=False)
    logging.info("Loaded stores.csv into stores_table")

    fsl_df = pd.read_csv(fsl_csv_path)
    fsl_df.to_sql("fsl_table", conn, if_exists="replace", index=False)
    logging.info("Loaded fsl.csv into fsl_table")

    conn.commit()
    conn.close()
    logging.info("Data loading completed")


def _create_view():
    logging.info("Creating view in SQLite database")
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    cursor.executescript(CREATE_VIEW_SQL)
    logging.info("Executed SQL script to create view")

    conn.commit()

    logging.info("View created successfully")
    cursor.execute("SELECT * FROM store_sales_view")
    rows = cursor.fetchall()

    column_names = [description[0] for description in cursor.description]

    logging.info("View data:")
    logging.info(f"Column names: {column_names}")

    for row in rows:
        logging.info(row)

    conn.close()
    logging.info("View creation and data selection completed")


def main():
    logging.info("Starting the process")
    _delete_db_if_exists(db_file_path)
    _load_data()
    _create_view()
    logging.info("Process completed successfully")


if __name__ == "__main__":
    main()
