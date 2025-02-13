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
    SUM(CASE WHEN strftime('%Y', d.DATE_FLD) = strftime('%Y', '{CURRENT_DATE}') THEN f.SALE_NET_VAL ELSE 0 END) AS YTD_SALES,
    -- Month-to-date sales
    SUM(CASE WHEN strftime('%Y-%m', d.DATE_FLD) = strftime('%Y-%m', '{CURRENT_DATE}') THEN f.SALE_NET_VAL ELSE 0 END) AS MTD_SALES,
    -- Week-to-date sales (Monday to Sunday)
    SUM(CASE WHEN d.DATE_FLD BETWEEN date('{CURRENT_DATE}', 'weekday 0', '-6 days') AND date('{CURRENT_DATE}', 'weekday 0') THEN f.SALE_NET_VAL ELSE 0 END) AS WTD_SALES
FROM
    stores_table s
JOIN
    fsl_table f ON s.STORE_KEY = f.STORE_KEY
JOIN
    date_table d ON f.DATE_KEY = d.DATE_KEY
GROUP BY
    s.STORE_KEY, s.STORE_CODE, s.STORE_DESCRIPTION
ORDER BY s.STORE_KEY, s.STORE_CODE, s.STORE_DESCRIPTION;
"""

CREATE_VIEW_SQL_YEARLY = f"""
CREATE VIEW store_sales_view_yearly AS
SELECT
    s.STORE_KEY,
    s.STORE_CODE,
    s.STORE_DESCRIPTION,
    strftime('%Y', d.DATE_FLD) as YEAR,
    SUM(f.SALE_NET_VAL) AS YEARLY_SALES
FROM
    stores_table s
JOIN
    fsl_table f ON s.STORE_KEY = f.STORE_KEY
JOIN
    date_table d ON f.DATE_KEY = d.DATE_KEY
GROUP BY
    s.STORE_KEY, s.STORE_CODE, s.STORE_DESCRIPTION, strftime('%Y', d.DATE_FLD)
ORDER BY s.STORE_KEY, s.STORE_CODE, s.STORE_DESCRIPTION, YEAR;
"""

CREATE_VIEW_SQL_MONTHLY = f"""
CREATE VIEW store_sales_view_monthly AS
SELECT
    s.STORE_KEY,
    s.STORE_CODE,
    s.STORE_DESCRIPTION,
    strftime('%Y-%m', d.DATE_FLD) as YEAR_MONTH,
    SUM(f.SALE_NET_VAL) AS MONTHLY_SALES
FROM
    stores_table s
JOIN
    fsl_table f ON s.STORE_KEY = f.STORE_KEY
JOIN
    date_table d ON f.DATE_KEY = d.DATE_KEY
GROUP BY
    s.STORE_KEY, s.STORE_CODE, s.STORE_DESCRIPTION, strftime('%Y-%m', d.DATE_FLD)
ORDER BY s.STORE_KEY, s.STORE_CODE, s.STORE_DESCRIPTION, YEAR_MONTH;
"""

CREATE_VIEW_SQL_WEEKLY = f"""
CREATE VIEW store_sales_view_weekly AS
SELECT
    s.STORE_KEY,
    s.STORE_CODE,
    s.STORE_DESCRIPTION,
    strftime('%Y', d.DATE_FLD) as YEAR,
    strftime('%W', d.DATE_FLD) as WEEK,
    SUM(f.SALE_NET_VAL) AS WEEKLY_SALES
FROM
    stores_table s
JOIN
    fsl_table f ON s.STORE_KEY = f.STORE_KEY
JOIN
    date_table d ON f.DATE_KEY = d.DATE_KEY
GROUP BY
    s.STORE_KEY, s.STORE_CODE, s.STORE_DESCRIPTION, strftime('%Y', d.DATE_FLD), strftime('%W', d.DATE_FLD)
ORDER BY s.STORE_KEY, s.STORE_CODE, s.STORE_DESCRIPTION, YEAR, WEEK;
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


def _print_view_results(cursor, view_name):
    cursor.execute(f"SELECT * FROM {view_name}")
    rows = cursor.fetchall()

    column_names = [description[0] for description in cursor.description]

    logging.info(f"View data for {view_name}:")
    logging.info(f"Column names: {column_names}")

    for row in rows:
        logging.info(row)


def _create_views():
    logging.info("Creating view in SQLite database")
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    cursor.executescript(CREATE_VIEW_SQL)
    cursor.executescript(CREATE_VIEW_SQL_YEARLY)
    cursor.executescript(CREATE_VIEW_SQL_MONTHLY)
    cursor.executescript(CREATE_VIEW_SQL_WEEKLY)
    logging.info("Executed SQL scripts to create view")

    conn.commit()

    logging.info("View created successfully")
    logging.info("Selecting data from view store_sales_view")
    _print_view_results(cursor, "store_sales_view")

    logging.info("Selecting data from view store_sales_view_yearly")
    _print_view_results(cursor, "store_sales_view_yearly")

    logging.info("Selecting data from view store_sales_view_monthly")
    _print_view_results(cursor, "store_sales_view_monthly")

    logging.info("Selecting data from view store_sales_view_weekly")
    _print_view_results(cursor, "store_sales_view_weekly")

    conn.close()
    logging.info("View creation and data selection completed")


def main():
    logging.info("Starting the process")
    _delete_db_if_exists(db_file_path)
    _load_data()
    _create_views()
    logging.info("Process completed successfully")


if __name__ == "__main__":
    main()
