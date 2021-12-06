import pandas as pd
import psycopg2 as sql
from pathlib import Path
from datetime import datetime as dt

LOCAL_FILESYSTEM_PATH = Path.cwd() / 'local'
DB_CREDENTIALS = dict(user="northwind_user",
                        password="thewindisblowing",
                        host="127.0.0.1",
                        port=5432,
                        database="northwind")

def today_str(format="%Y-%m-%d"):
    """System today date as string

        Args:
            format (str, optional): Date string format. Defaults to "%Y-%m-%d".

        Returns:
            str: Today date"""

    return dt.today().strftime(format=format)

def save_to_local_filesystem(date, user, password, host, port, database, debug=False):
    """Saves remote database to local filesystem

        Args:
            date (str): Date of data retrival (%Y-%m-%d)
            user (str): Database user credential
            password (str): Dataabase password credential
            host (str): Database host
            port (str or int): Database port
            database (str): Database name
            debug (bool, optional): Show debugging prints. Defaults to False."""

    connection=None # needed for finally statement
    print(f"Connecting to {database} database...")
    try:
        connection = sql.connect(user=user,
                                    password=password,
                                    host=host,
                                    port=port,
                                    database=database)
        print("Connection successfull!")

        cursor = connection.cursor()

        print("Querying database for public tables")
        postgreSQL_select_Query = "select table_name from information_schema.tables where table_schema='public'"
        cursor.execute(postgreSQL_select_Query)

        print("Selecting rows from table using cursor.fetchall")
        query_response = cursor.fetchall()
        if debug:
            print(f"Tables found: {query_response}")
        

        print("Saving tables to local filesystem")
        for table_data in query_response:
            table_name = table_data[0]
            df = pd.read_sql(f"select * from {table_name}", connection)
            path = LOCAL_FILESYSTEM_PATH / 'postgre' / date
            path.mkdir(parents=True, exist_ok=True)
            df.to_csv(path / f"{table_name}.csv")

    finally:
        if connection:
            cursor.close()
            connection.close()
            print(f"PostgreSQL connection to {database} database is closed")

    print("Loading order_details CSV data...")
    order_details_df = pd.read_csv('data/order_details.csv')
    print("And saving it in the local filesystem")
    path = LOCAL_FILESYSTEM_PATH / 'csv' / date
    path.mkdir(parents=True, exist_ok=True)
    order_details_df.to_csv(path / f"order_details.csv")
    print("All done.")
    return

if __name__ == "__main__":
    save_date = today_str()
    save_to_local_filesystem(save_date)
