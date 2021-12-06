from pathlib import Path
import psycopg2 as sql
import pandas as pd

import step_one
step_one.LOCAL_FILESYSTEM_PATH = Path.cwd() / 'local3'

import step_two

if __name__=='__main__':

    date = step_one.today_str()

    step_one.save_to_local_filesystem(date, **step_one.DB_CREDENTIALS)

    step_two.export_to_database(date, **step_two.DB_CREDENTIALS)

    with sql.connect(**step_two.DB_CREDENTIALS) as connection:
        for table in ['orders','order_details']:
            df = pd.read_sql(f"select * from {table}", con=connection)
            df.to_csv(f'final_query_{table}.csv')