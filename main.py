import argparse
import os
import json
import sqlite3

parser = argparse.ArgumentParser()
parser.add_argument('--name', '-n', type=str,
                    help='sets the database name. For example: -n db', required=True)
parser.add_argument('--table', '-t', type=str,
                    help='sets the table name. For example: -t product')
parser.add_argument('--columns', '-c', type=json.loads,
                    help="sets the name of the columns. For example: -c '{\"column_name\": \"column_type\"}'")
parser.add_argument('--file', '-f', type=str,
                    help='sets the name of the file from which the table is populated. For example: -f data.txt')
parser.add_argument('--recreation', '-re', type=bool,
                    help='Indicates whether to create the table again or not. For example: -re True')

ARG = parser.parse_args()


def create_database(name: str=ARG.name, table: str=ARG.table,
                    columns: dict=ARG.columns, recreation: bool=ARG.recreation,file: str=ARG.file):
    # Deletes a database if --recreation is True.
    if recreation:
        try:
            os.remove(name)
            print('Database removed. Reacreation...')
        except FileNotFoundError:
            pass

    # Database connection.
    with sqlite3.connect(name) as connect:
        cur = connect.cursor()
        # Creates table if it does not exists.
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                {', '.join(f'{key} {value}' for key, value in columns.items())}
            )
            """
        )

        # If --file not empty, writes data from the specified file.
        if file:
            try:
                with open(file, 'r') as file:
                    for row in file.readlines():
                        cur.execute(
                            f"""
                            INSERT INTO {table}
                            ({', '.join(column for column in columns.keys())})
                            VALUES
                            ({', '.join([item for item in row.rstrip().split()])})
                            """
                        )
            except FileNotFoundError as err:
                print(f'ERROR: {err}')

        connect.commit()


if __name__ == '__main__':
    create_database()
