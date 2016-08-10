#!/usr/bin/env python3
import MySQLdb
import json
import argparse

INPUT = {
    "database": "mysql",
    "table": "user"
}


def main():
    parser = argparse.ArgumentParser(description='MySQL desc to BQ schema',
                                     add_help=False)
    parser.add_argument('--help', action='help', help='help')
    parser.add_argument('-u', type=str, help='mysql user')
    parser.add_argument('-h', type=str, help='mysql host')
    parser.add_argument('-p', type=str, help='mysql password')
    args = parser.parse_args()

    HOST = args.h
    PW = args.p
    USER = args.u

    create_bq_schema(HOST, USER, PW, INPUT['database'], INPUT['table'])


def create_bq_schema(host, user, password, db, table):

    connector = MySQLdb.connect(
        host=host,
        user=user,
        passwd=password,
        db=db
    )

    ret = []

    cursor = connector.cursor()
    cursor.execute("desc " + table)

    for row in cursor.fetchall():
        ret.append(
            {
                "name": row[0],
                "type": convert_type(row[1])
            }
        )

    cursor.close
    connector.close

    filename = "_".join([INPUT['database'], INPUT['table']]) + ".json"

    with open(filename, mode='w') as f:
        f.write(json.dumps(ret))


def convert_type(original):
    if "int" in original:
        return "INTEGER"

    return "STRING"


if __name__ == "__main__":
    main()
