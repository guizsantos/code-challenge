import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime as dt
from step_one import LOCAL_FILESYSTEM_PATH

DB_CREDENTIALS = dict(user='dieklaxjpbzjwx',
                        password='8ab852b6562f72e7239858c2cc2d1bc2abf591878c4eb543adb2dd782389a621',
                        host='ec2-34-205-14-168.compute-1.amazonaws.com',
                        port=5432,
                        database='daarcpq5erp96c')
connection_str = "postgresql://{user}:{password}@{host}:{port}/{database}"

def export_to_database(date, user, password, host, port, database, debug=False, overwrite=False):
    """Export local filesystem to remote database

        Args:
            date (str): Date of data retrival (%Y-%m-%d)
            user (str): Database user credential
            password (str): Dataabase password credential
            host (str): Database host
            port (str or int): Database port
            database (str): Database name
            debug (bool, optional): Show debugging prints. Defaults to False.
            overwrite (bool, optional): Overwrite data on remote database. Defaults to False."""
    
    print(f"Connecting to {database} database...")
    engine = create_engine(connection_str.format(user=user,password=password,host=host,port=port,database=database))

    print(f"Saving local filesystem from {date} to {database} database...")
    for dir_name, sub_dirs, files in os.walk(LOCAL_FILESYSTEM_PATH, topdown=False):
        for file in files:
            if file.endswith('csv'):
                if debug:
                    print(f"Exporting {file} from {dir_name} to {database} database.")
                df = pd.read_csv(f'{dir_name}\\{file}')
                table_name = file.split('.')[0]
                try:
                    df.to_sql(name=table_name, con=engine, if_exists='replace' if overwrite else 'fail', index=False)
                    print(f"Sucessfully uploaded {table_name} table to {database} database.")
                except ValueError:
                    print(f"Table {table_name} already exists, replacing values will be a destructive operation. Rerun with overwrite=True to replace current database with new values.")
    return

if __name__ == "__main__":
    from step_one import today_str
    save_date = today_str()
    export_to_database(save_date, debug=True)
