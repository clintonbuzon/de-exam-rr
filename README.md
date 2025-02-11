# README

## You can run this on GitHub Codespaces or your local environment

### GitHub Codespace

Create a codespace on the `main` branch.

![Screenshot1](Screenshots/Github_codespace_1.png)

Run commands in the codespace terminal.

![Screenshot2](Screenshots/Github_codespace_2.png)


## Prerequisites

Before running the solutions or tests, ensure that you have the following installed:

- Python 3.x
- `pip` (Python package installer)

You can install the required Python packages by running:

```sh
pip install pytest pytest-cov pandas
```

## Developer Notes

- Both test items require some sort of database.
- I’ve chosen to use an SQLite database since it comes with Python by default, and the requirements are light.
- This approach saves time for the reviewer, as they won’t need to set up a database, host, ports, etc., just to run the solutions.
- I’ve also used pandas in one of the solutions for ease of use, as it is a reliable standard for processing small amounts of data without the need for distributed processing.
- I would have opted for pyspark if the data processed were massive.


## Running the Solutions

### Item 1

Note: The provided events.ndjson file appears to be corrupted, with missing brackets and lines that aren't in JSON format. I’ve manually cleaned this file so that the source data is in a proper format.

To run the solution.py file in Item 1, follow these steps:
```sh
cd "Item 1"
python solution.py
```

This will:

- Create an SQLite database.
- Process the NDJSON events file from source/events.ndjson and load it into the database.
- Export the results to target/events.csv.

Optionally, you can run the following command to start an SQL terminal on the created database:

```sh
sqlite3 events.db
SELECT * FROM events_table;
```

### Item 2

To run the solution.py file in Item 2, follow these steps:

```sh
cd "Item 2"
python solution.py
```

This will:

- Create an SQLite database.
- Load data from CSV files (date.csv, stores.csv, fsl.csv).
- Create a view with YTD/MTD/WTD sales calculations.
- Print out the results from the view.

Optionally, you can run the following command to start an SQL terminal on the created database:

```sh
sqlite3 sales.db
SELECT * FROM store_sales_view;
```

## Running Tests

To run the tests for both Item 1 and Item 2 using pytest with coverage reporting, follow these steps:

### Item 1

```sh
cd "Item 1"
pytest -v --cov=solution --cov-report=term-missing
```

### Item 2

```sh
cd "Item 2"
pytest -v --cov=solution --cov-report=term-missing
```