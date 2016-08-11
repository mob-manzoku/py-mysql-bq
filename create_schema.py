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

        # print(json.dumps(schemas))

    conn.close


def create_sql(table, schemas):

    colomns = ",".join([s['name'] for s in schemas])
    return "SELECT " + colomns + " FROM " + table


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

    if original.startswith("bool"):
        return "BOOLEAN"
    if original.startswith("boolien"):
        return "BOOLEAN"
    if original.startswith("tinyint(1)"):
        # print("tinuint(1) as bool")
        return "BOOLEAN"

    if original.startswith("tinyint"):
        return "INTEGER"
    if original.startswith("smallint"):
        return "INTEGER"
    if original.startswith("mediumint"):
        return "INTEGER"
    if original.startswith("int"):
        return "INTEGER"
    if original.startswith("bigint"):
        return "INTEGER"

    if original.startswith("float"):
        return "FLOAT"
    if original.startswith("double"):
        return "FLOAT"
    if original.startswith("decimal"):
        return "FLOAT"

    if original.startswith("char"):
        return "STRING"
    if original.startswith("varchar"):
        return "STRING"
    if original.startswith("text"):
        return "STRING"

    if original.startswith("timestamp"):
        return "TIMESTAMP"

    if original.startswith("binary"):
        return "BYTES"
    if original.startswith("varbinary"):
        return "BYTES"

    print(original)
    return "STRING"


def file_out(schemas, db, table, prefix, suffix):
    filename = "_".join([db, table])

    if prefix != "":
        filename = prefix + "_" + filename

    if suffix != "":
        filename = filename + "_" + suffix

    with open("format/"+filename+".json", mode='w') as f:
        f.write(json.dumps(schemas))

    with open("sql/"+filename+".sql", mode='w') as f:
        f.write(create_sql(table, schemas))


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
                        help='Set a table name, if no set, use all tables')
    parser.add_argument('--prefix', default="", type=str,
                        help='Add prefix like directories')
    parser.add_argument('--suffix', default="", type=str,
                        help='Add suffix, default is ".json"')

    return parser.parse_args()


if __name__ == "__main__":
    main()
