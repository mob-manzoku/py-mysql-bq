#!/usr/bin/env python3
import MySQLdb
import json
import argparse


def main():
    args = define_parsers()
    create_bq_schema(args.host, args.user, args.passwd, args.db, args.table)


def define_parsers():
    parser = argparse.ArgumentParser(description='MySQL desc to BQ schema',
                                     add_help=False)
    parser.add_argument('--help', action='help', help='help')

    parser.add_argument('-u', '--user', type=str, default="root",
                        help='mysql user')
    parser.add_argument('-h', '--host', type=str, default="localhost",
                        help='mysql host')
    parser.add_argument('-p', '--passwd', type=str, default="",
                        help='mysql password')
    parser.add_argument('db', type=str,
                        help='mysql database name')
    parser.add_argument('--table', type=str,
                        help='mysql table name')

    return parser.parse_args()


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

    filename = "_".join([db, table]) + ".json"

    with open(filename, mode='w') as f:
        f.write(json.dumps(ret))


def convert_type(original):
    if "int" in original:
        return "INTEGER"

    return "STRING"


if __name__ == "__main__":
    main()
