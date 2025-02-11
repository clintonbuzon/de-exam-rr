# README

## You can run this on github codespaces or your local environment

### Github codespace

Create codespace on main

![Screenshot1](Screenshots/Github_codespace_1.png)

Run commands in codespace terminal

![Screenshot2](Screenshots/Github_codespace_2.png)


## Prerequisites

Before running the solutions or tests, ensure you have the following installed:

- Python 3.x
- `pip` (Python package installer)

Install the required Python packages by running:

```sh
pip install pytest pytest-cov pandas
```

## Developer notes

- Both test items require some sort of database
- I've opted with using SQLite database since this is already comes with python and the requirements are light 
- This saves time for the reviewer so that they wouldn't need to setup a database/host/ports/etc just to run the solutions
- Also used `pandas` in one of the solution for ease of use and since `pandas` is one of the reliable standards when it comes to processing small amounts of data without the need for distributed processing
- Would have opted for `pyspark` if data processed is massive


## Running the Solutions

### Item 1

Note: the provided `events.ndjson` seems to be corrupted with missing brackets and lines not in json format. I've cleaned this manually so that source data is clean.

To run the `solution.py` file in `Item 1`, follow these steps:

```sh
cd "Item 1"
python solution.py
```

This will:

- Create a SQLite database
- Process the NDJSON events file from `source/events.ndjson` to database
- Export results to target/events.csv

Optionally you can run the following command to start sql terminal on the created database:

```sh
sqlite3 events.db
SELECT * FROM events_table;
```

### Item 2

To run the `solution.py` file in `Item 2`, follow these steps:

```sh
cd "Item 2"
python solution.py
```

This will:

- Create a SQLite database
- Load data from CSV files (date.csv, stores.csv, fsl.csv)
- Create view with YTD/MTD/WTD sales calculations
- Print out results of view

Optionally you can run the following command to start sql terminal on the created database:

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