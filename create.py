#!/usr/bin/env python3
import MySQLdb
import json
import argparse


def main():
    args = define_parsers()

    db = args.db
    conn = create_db_connection(args.host, args.user, args.passwd, db)

    if args.table is None:
        tables = get_all_tables(conn)

    else:
        tables = [args.table]

    for t in tables:
        schemas = create_bq_schema(conn, t)
        file_out(schemas, db, t, args.prefix, args.suffix)

    conn.close


def create_db_connection(host, user, password, db):

    connector = MySQLdb.connect(
        host=host,
        user=user,
        passwd=password,
        db=db
    )

    return connector


def get_all_tables(conn):
    ret = []

    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")

    for row in cursor.fetchall():
        ret.append(row[0])

    cursor.close

    return ret


def create_bq_schema(conn, table):
    ret = []

    cursor = conn.cursor()
    cursor.execute("DESC " + table)

    for row in cursor.fetchall():
        ret.append(
            {
                "name": row[0],
                "type": convert_type(row[1])
            }
        )

    cursor.close

    return ret


def convert_type(original):
    if "int" in original:
        return "INTEGER"

    return "STRING"


def file_out(schemas, db, table, prefix, suffix):
    filename = "_".join([db, table])
    filename = filename + suffix

    if prefix != "":
        filename = prefix + "_" + filename

    with open(filename, mode='w') as f:
        f.write(json.dumps(schemas))


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
                        help='Set a table name, if not set, all tables')
    parser.add_argument('--prefix', default="", type=str,
                        help='Add prefix like a project code')
    parser.add_argument('--suffix', default=".json", type=str,
                        help='Add suffix, default is ".json"')

    return parser.parse_args()


if __name__ == "__main__":
    main()
