#   Insert results into a table within the database. Add option --norescan to query only from the stats table and skip truncating & repopulating table.
        # Truncate the stats table before each run when --noscan is not given.
        # Add option to set commit rate. Allows control of long running transactions
#   pgstattuple works for both tables and indexes. pgstatindex gives more detailed info about indexes

#!/usr/bin/env python

# Script is maintained at https://github.com/keithf4/pg_bloat_check

import argparse, psycopg2, sys
from psycopg2 import extras

version = "2.0.0"

# Bloat queries are adapted from the check_bloat query found in bucardo's check_postgres tool http://bucardo.org/wiki/Check_postgres

parser = argparse.ArgumentParser(description="Provide a bloat report for PostgreSQL tables and/or indexes. This script uses the pgstattuple contrib module which must be installed first. Note that the query to check for bloat can be extremely expensive on very large databases or those with many tables. The script stores the bloat stats in a table so they can be queried again as needed without having to re-run the entire scan. The table contains a timestamp columns to show when it was obtained.")
args_general = parser.add_argument_group(title="General options")
args_general.add_argument('-c','--connection', default="host=", help="""Connection string for use by psycopg. Defaults to "host=" (local socket).""")
# TODO see about a possible table format
args_general.add_argument('-e', '--exclude_object_file', help="""Full path to file containing a return deliminated list of objects to exclude from the report (tables and/or indexes). All objects must be schema qualified. Comments are allowed if the line is prepended with "#".""")
args_general.add_argument('-f', '--format', default="simple", choices=["simple", "dict"], help="Output formats. Simple is a plaintext version suitable for any output (ex: console, pipe to email). Dict is a python dictionary object, which may be useful if taking input into another python script or something that needs a more structured format. Dict also provides more details about dead tuples, empty space & free space. Default is simple.")
args_general.add_argument('-m', '--mode', choices=["tables", "indexes", "both"], default="both", help="""Provide bloat reports for tables, indexes or both. Note that table bloat statistics do not include index bloat in their results. Index bloat is always distinct from table bloat and reported separately. Default is "both".""")
args_general.add_argument('-n', '--schema', help="Comma separated list of schema to include in report. All other schemas will be ignored.")
args_general.add_argument('-N', '--exclude_schema', help="Comma separated list of schemas to exclude.")
args_general.add_argument('-p', '--min_wasted_percentage', type=float, default=0.1, help="Minimum percentage of wasted space an object must have to be included in the report. Default and minimum value is 0.1 (DO NOT include percent sign in given value).")
args_general.add_argument('-q', '--quick', action="store_true", help="Use the pgstattuple_approx() function instead of pgstattuple() for a quicker, but possibly less accurate bloat report. Note this only works in PostgreSQL 9.5+")
args_general.add_argument('-r', '--commit_rate,', type=int, default=10, help="Sets how many tables are scanned before commiting inserts into the bloat table. Helps avoid long running transactions when scanning large tables. Default is 10. Set to 0 to avoid committing until all tables are scanned. NOTE: The bloat table is truncated on every run unless --noscan is set. The truncate is permanent after the first commit.")
args_general.add_argument('-s', '--min_size', type=int, default=1, help="Minimum size in bytes of object to scan (table or index). Default and minimum value is 1.")
args_general.add_argument('--version', action="store_true", help="Print version of this script.")
args_general.add_argument('-z', '--min_wasted_size', type=int, default=1, help="Minimum size of wasted space in bytes. Default and minimum is 1.")
args_general.add_argument('--debug', action="store_true", help="Output additional debugging information.")

args_setup = parser.add_argument_group(title="Setup")
args_setup.add_argument('--pgstattuple_schema', help="If pgstattuple is not installed in the default search path, use this option to designate the schema where it is installed.")
args_setup.add_argument('--bloat_schema', help="Set the schema that the bloat report table is in if it's not in the default search path. Note this option can also be set when running --create_stats_table to set which schema you want the table created.")
args_setup.add_argument('--create_stats_table', action="store_true", help="Create the required tables that the bloat report uses (bloat_stats + two child tables). Places table in default search path unless --bloat_schema is set.")
args = parser.parse_args()


def check_pgstattuple(conn):
    sql = "SELECT count(*) FROM pg_catalog.pg_extension WHERE extname = 'pgstattuple'"
    cur = conn.cursor()
    cur.execute(sql)
    exists = cur.fetchone()[0]
    if exists < 1:
        print("pgstattuple extension not found. Please ensure it is installed in the database this script is connecting to.")
        sys.exit(2)


def create_conn():
    conn = psycopg2.connect(args.connection)
    return conn


def close_conn(conn):
    conn.close()


def create_list(list_type, list_items):
    split_list = []
    if list_type == "csv":
        split_list = list_items.split(',')
    elif list_type == "file":
        try:
            fh = open(list_items, 'r')
            for line in fh:
                if not line.strip().startswith('#'):
                    split_list.append(line.strip())
        except IOError as e:
           print("Cannot access exclude file " + list_items + ": " + e.strerror)
           sys.exit(2)

    return split_list


def create_bloat_table(conn):
    create_sql = "CREATE TABLE IF NOT EXISTS "
    if args.bloat_schema != None:
        create_sql += args.view_schema + "."
    sql = create_sql + """bloat_stats (schemaname text NOT NULL
                            , objectname text NOT NULL
                            , objecttype text NOT NULL
                            , size_bytes bigint
                            , live_tuple_count bigint
                            , live_tuple_percent float8
                            , dead_tuple_count bigint
                            , dead_tuple_size_bytes bigint
                            , dead_tuple_percent float8
                            , free_space_bytes bigint
                            , free_percent float8
                            , stats_timestamp timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
                            , approximate boolean NOT NULL DEFAULT false)"""
    cur = conn.cursor()
    cur.execute(sql)
    sql = create_sql + "bloat_tables (LIKE bloat_stats INCLUDING ALL) INHERITS (bloat_stats)"
    cur.execute(sql)
    sql = create_sql + "bloat_indexes (LIKE bloat_stats INCLUDING ALL) INHERITS (bloat_stats)"
    cur.execute(sql)
    sql = "COMMENT ON TABLE bloat_stats IS 'Table providing raw data for table & index bloat'"
    cur.execute(sql)
    sql = "COMMENT ON TABLE bloat_tables IS 'Table providing raw data for table bloat'"
    cur.execute(sql)
    sql = "COMMENT ON TABLE bloat_indexes IS 'Table providing raw data for index bloat'"
    cur.execute(sql)

    conn.commit()
    cur.close()


def get_bloat(conn, exclude_schema_list, include_schema_list, exclude_object_list):
    sql = ""
    sql_class = """SELECT c.oid, c.relkind, c.relname, n.nspname 
                    FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace """
                    
    if args.mode == "both":
        sql_class += " WHERE relkind IN ('r', 'i', 'm') "
    elif args.mode == "tables":
        sql_class += " WHERE relkind IN ('r', 'm') "
    elif args.mode == "indexes":
        sql_class += " WHERE relkind IN ('i') "

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # IN clauses work with python tuples. lists were converted by get_bloat() call
    if include_schema_list:
        sql_class += " AND n.nspname IN %s"
        if args.debug:
            print(cur.mogrify(sql_class, (include_schema_list,)))
        cur.execute(sql_class, (include_schema_list,))
    elif exclude_schema_list:
        sql_class += " AND n.nspname NOT IN %s"
        if args.debug:
            print("sql_class: " + cur.mogrify(sql_class, (exclude_schema_list,) ))
        cur.execute(sql_class, (exclude_schema_list,) )
    else:
        cur.execute(sql)

    object_list = cur.fetchall()

    sql = "TRUNCATE "
    if args.bloat_schema:
        sql += args.view_schema + "."
    if args.mode == "tables" or args.mode == "both":
        sql_table = sql + "bloat_tables"
        cur.execute(sql_table)
    if args.mode == "indexes" or args.mode == "both":
        sql_index = sql + "bloat_indexes"
        cur.execute(sql_index)

    if args.quick:
        approximate = True
    else:
        approximate = False

    for o in object_list:
        if args.debug:
            print(o)
        if exclude_object_list:
            match_found = False
            for e in exclude_object_list:
                print("list object: " + e)
                print("class object: " + o['nspname'] + "." + o['relname'])
                if e == o['nspname'] + "." + o['relname']:
                    match_found = True
            if match_found:
                continue

        if args.quick:
            sql = "SELECT table_len, approx_tuple_count AS tuple_count, approx_tuple_len AS tuple_len, approx_tuple_percent AS tuple_percent, dead_tuple_count,  "
            sql += "dead_tuple_len, dead_tuple_percent, approx_free_space AS free_space, approx_free_percent AS free_percent FROM "
        else:
            sql = "SELECT table_len, tuple_count, tuple_len, tuple_percent, dead_tuple_count, dead_tuple_len, dead_tuple_percent, free_space, free_percent FROM "
        if args.bloat_schema != None:
            sql += " \"" + arg.pgstattuple_schema + "\"."
        if args.quick:
            sql += "pgstattuple_approx(%s::regclass) "
        else:
            sql += "pgstattuple(%s::regclass) "

        sql += " WHERE table_len > %s"
        sql += " AND (dead_tuple_len + free_space) > %s"
        sql += " AND dead_tuple_percent + free_percent > %s"
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        if args.debug:
            print("sql: " + cur.mogrify(sql, [o['oid'], args.min_size, args.min_wasted_size, args.min_wasted_percentage]))
        cur.execute(sql, [o['oid'], args.min_size, args.min_wasted_size, args.min_wasted_percentage])
        stats = cur.fetchall()

        if args.debug:
            print(stats)

        if stats: # completely empty objects will be zero for all stats, so this would be an empty set
            sql = "INSERT INTO "
            if args.bloat_schema != None:
                sql += args.view_schema + "."

            if o['relkind'] == "r" or o['relkind'] == "m":
                sql+= "bloat_tables"
                if o['relkind'] == "r":
                    objecttype = "table"
                else:
                    objecttype = "materialized view"
            elif o['relkind'] == "i":
                sql+= "bloat_indexes"
                objecttype = "index"
                
            sql += """ (schemaname
                        , objectname 
                        , objecttype
                        , size_bytes
                        , live_tuple_count
                        , live_tuple_percent
                        , dead_tuple_count
                        , dead_tuple_size_bytes
                        , dead_tuple_percent
                        , free_space_bytes
                        , free_percent
                        , approximate)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
            if args.debug:
                print("insert sql: " + cur.mogrify(sql, [     o['nspname']
                                                            , o['relname']
                                                            , objecttype 
                                                            , stats[0]['table_len']
                                                            , stats[0]['tuple_count']
                                                            , stats[0]['tuple_percent']
                                                            , stats[0]['dead_tuple_count']
                                                            , stats[0]['dead_tuple_len']
                                                            , stats[0]['dead_tuple_percent']
                                                            , stats[0]['free_space']
                                                            , stats[0]['free_percent']
                                                            , approximate
                                                        ])) 
            cur.execute(sql, [   o['nspname']
                               , o['relname']
                               , objecttype
                               , stats[0]['table_len']
                               , stats[0]['tuple_count']
                               , stats[0]['tuple_percent']
                               , stats[0]['dead_tuple_count']
                               , stats[0]['dead_tuple_len']
                               , stats[0]['dead_tuple_percent']
                               , stats[0]['free_space']
                               , stats[0]['free_percent']
                               , approximate
                             ]) 


def print_report(result_list):
    for r in result_list:
        print(r)

def print_version():
    print("Version: " + version)


if __name__ == "__main__":
    if args.version:
        print_version()
        sys.exit(1)

    if args.schema != None and args.exclude_schema != None:
        print("--schema and --exclude_schema are exclusive options and cannot be set together")
        sys.exit(2)

    conn = create_conn()

    check_pgstattuple(conn)

    if args.create_stats_table:
        create_bloat_table(conn)
        close_conn(conn)
        sys.exit(1)

    if args.exclude_schema != None:
        exclude_schema_list = create_list('csv', args.exclude_schema)
    else:
        exclude_schema_list = []
    exclude_schema_list.append('pg_toast')

    if args.schema != None:
        include_schema_list = create_list('csv', args.schema)
    else:
        include_schema_list = []

    if args.exclude_object_file != None:
        exclude_object_list = create_list('file', args.exclude_object_file)
    else:
        exclude_object_list = []

    get_bloat(conn, tuple(exclude_schema_list), tuple(include_schema_list), exclude_object_list)
# TODO REMOVE - taken care of in get_bloat()
#    if args.mode == "tables":
#        result = get_table_bloat(conn, include_schema_list, exclude_object_list)
#    if args.mode == "indexes":
#        result = get_index_bloat(conn, include_schema_list, exclude_object_list)

    # Final commit in case --commit_rate is lower than rows inserted
    conn.commit()
    close_conn(conn)

    counter = 1
    result_list = []
##    for r in result:
        # Min check goes in order page, wasted_page, wasted_size, wasted_percentage to exclude things properly when options are combined
##        if args.min_pages > 1:
##            if r['pages'] < args.min_pages:
##                continue
##        elif args.min_pages < 1:
##            print("--min_pages (-a) must be >= 1")
##            sys.exit(2)

##        if args.min_wasted_size > 1:
##            if r['wastedbytes'] < args.min_wasted_size:
##                continue
##        elif args.min_wasted_size < 1:
##            print("--min_wasted_size (-z) must be >= 1")
##            sys.exit(2)

##        if float(args.min_wasted_percentage) > float(0.1):
##            if float(r['bloat_percent']) < float(args.min_wasted_percentage):
##                continue
##        elif float(args.min_wasted_percentage) < float(0.1):
##            print("--min_wasted_percentage (-p) must be >= 0.1")
##            sys.exit(2)

# TODO REMOVE. DO filtering during running of query so it actually skips getting those stats
#        if r['schemaname'] in exclude_schema_list:
#            continue
#
#        if ( len(include_schema_list) > 0 and r['schemaname'] not in include_schema_list ):
#            continue
#
#        if ( len(exclude_object_list) > 0 and
#                (r['schemaname'] + "." + r['objectname']) in exclude_object_list ):
#            continue
# TODO REMOVE ABOVE

##        if args.format == "simple":
##            justify_space = 100 - len(str(counter)+". "+r['schemaname']+"."+r['objectname']+"(%)"+str(r['bloat_percent'])+r['wastedsize']+" wasted")
##            result_list.append(str(counter) + ". " + r['schemaname'] + "." + r['objectname'] + "."*justify_space + "(" + str(r['bloat_percent']) + "%) " + r['wastedsize'] + " wasted")
##            counter += 1
##        elif args.format == "dict":
##            result_dict = dict([('schemaname', r['schemaname'])
##                                , ('objectname', r['objectname'])
##                                , ('total_pages', int(r['pages']) )
##                                , ('bloat_percent', str(r['bloat_percent'])+"%" )
##                                , ('wasted_size', r['wastedsize'])
##                                , ('wasted_pages', int(r['wastedpages']))
##                                ])
##            result_list.append(result_dict)
##
##    if len(result_list) >= 1:
##        print_report(result_list)
##    else:
##        print("No bloat found for given parameters")

"""
LICENSE AND COPYRIGHT
---------------------

pg_bloat_check.py is released under the PostgreSQL License, a liberal Open Source license, similar to the BSD or MIT licenses.

Copyright (c) 2016 Keith Fiske

Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and without a written agreement is hereby granted, provided that the above copyright notice and this paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
"""
