#!/usr/bin/python:
import sys, re,  time, argparse
import mysql.connector
from itertools import islice, chain
from random import randint
from prettytable import PrettyTable

# Arguments to initiate the script
prefix = "000-008-"
start_from = 0
end_at = 1000


class Color:
    def __init__(self):
        pass

    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


class DBArchive:
    def __init__(self):
        self.batch_summary = None
        self.total_rows_affected = None
        self.src_db_cnx = None
        self.MAIN_TABLE_DATA = None
        self.MAIN_PK_COL = None
        self.ROWS_AFFECTED = None
        self.sub_tables = None
        self.child_table_ids = None
        self.tables_copy_time = []
        self.tables_del_time = []
        self.long_op_time = 2
        self.sub_batch_size = 100
        self.show_stats = 1
        self.explain = 0
        self.tables_list = dict()
        self.BATCH_ID = None

    def log_status(self, msg):
        print(msg)

    def end_sub(self, msg):
        print('\033[91m' + str(time.ctime()) + str(msg) + '\033[0m')
        sys.exit(1)

    def bold(self, msg):
        return u'\033[1m%s\033[0m' % msg

    def batch(self, iterable, size):
        sourceiter = iter(iterable)
        while True:
            batchiter = islice(sourceiter, size)
            yield chain([batchiter.next()], batchiter)

    # Update batch summary
    def update_stats(self, pkey, tbl, rows_copied, rows_deleted, time_spent, parent_tbl, rows_found=0):
        r_copied = 0
        r_del = 0
        r_time = 0
        r_found = 0
        for k, v in self.batch_summary.items():
            if pkey == k:
                r_copied = self.batch_summary[k][1]
                r_del = self.batch_summary[k][2]
                r_time = self.batch_summary[k][3]
                r_found = self.batch_summary[k][5]
        try:
            total_rows_copied = r_copied + rows_copied
            total_rows_deleted = r_del + rows_deleted
            total_time_spent = r_time + time_spent
            total_rows_found = r_found + rows_found
            self.batch_summary[pkey] = [tbl, total_rows_copied, total_rows_deleted, total_time_spent, parent_tbl,
                                        total_rows_found]
        except TypeError:
            print (str(r_copied) + " -- " + str(rows_copied))

    # Archive data
    def start_archive(self):
        self.batch_summary = dict()
        # exit function when nothing to archive
        if self.MAIN_TABLE_DATA.strip() == "-11":
            return
        rows_deleted = 0
        rows_copied = 0
        rows_deleted = 0
        rows_found = 0
        sorted_dict = sorted(self.tables_list.keys(), reverse=True)
        for k in sorted_dict:
            rows_deleted = 0
            rows_copied = 0
            rows_deleted = 0
            rows_found = 0
            tbl = str(self.tables_list[k][0])
            values = str(self.tables_list[k][3])
            col = str(self.tables_list[k][2])
            parent_tbl = str(self.tables_list[k][1])
            if values.strip() == "-11":
                self.update_stats(k, tbl, rows_copied, rows_deleted, 0, parent_tbl)
                continue
            ids = values.split(',')
            # Archive data in batches
            batch_size = self.sub_batch_size
            it = 0
            for vals in self.batch(ids, int(batch_size)):
                start_time = time.time()
                rows_copied = 0
                rows_deleted = 0
                where = "" + col + " IN(" + ",".join(vals) + ")"

                rs = self.run_sql("SELECT COUNT(*) FROM " + self.MAIN_DATABASE + "." + tbl + " WHERE " + where)
                # print("Sub-tables loop pass:" + str(it))
                for r in rs:
                    rows_found = r[0]
                if rows_found == 0:
                    self.update_stats(k, tbl, rows_copied, rows_deleted, 0, parent_tbl)
                    continue
                else:
                    self.update_stats(k, tbl, rows_copied, rows_deleted, 0, parent_tbl, rows_found)
                rows_deleted = self.del_data(tbl, where, rows_found)
                elapsed_time = time.time() - start_time
                # Generate batch summary
                self.update_stats(k, tbl, rows_found, rows_deleted, round(elapsed_time, 2), parent_tbl)
                it = it + 1

        # Archive data from MAIN TABLE
        start_time = time.time()
        rows_deleted = 0
        where = " " + self.MAIN_PK_COL + " IN(" + self.MAIN_TABLE_DATA + ")"
        if self.MAIN_TABLE_DATA.strip() != "-11":  # and self.DRY_RUN!="true": # and self.explain==0:
            rows_deleted = self.del_data(self.MAIN_TABLE, where, len(self.MAIN_TABLE_DATA.split(",")))
            self.update_stats(0, self.MAIN_TABLE, len(self.MAIN_TABLE_DATA.split(",")), rows_deleted, 0, '',
                              len(self.MAIN_TABLE_DATA.split(",")))
        elapsed_time = time.time() - start_time

        # Generate batch summary for MAIN_TABLE

        # Display stats/summary
        table = PrettyTable(["#", "Table", "Rows found", "Rows deleted", "Time spent", "Parent Table"])
        table.align = "l"
        for k, v in self.batch_summary.items():
            table.add_row([k, v[0], v[1], v[2], str(v[3]) + "s", v[4]])
        if len(self.batch_summary.items()) > 0 and self.VERBOSE == "1" and self.show_stats == 1:
            table.get_string(sortby="#")
            self.log_status(table)
        return rows_deleted

    def del_data(self, tbl, where, rows_found):
        start_time = time.time()
        rowcount = 0
        loop_itr = 0
        sql = "DELETE FROM " + self.MAIN_DATABASE + "." + tbl + " WHERE " + where + " LIMIT 1000"

        if self.DRY_RUN == "true":
            sql = "DELETE FROM " + self.MAIN_DATABASE + "." + tbl + " WHERE " + where
            if len(sql) > 500:
                self.log_status(sql[0:500] + ") /* output truncated ...*/\n")
            else:
                self.log_status(sql)
            return rowcount

        while 1:
            rs = self.run_sql(sql)
            self.total_rows_affected = self.total_rows_affected + self.ROWS_AFFECTED
            rowcount += self.ROWS_AFFECTED
            loop_itr += 1
            if loop_itr % 10 == 0 and self.VERBOSE == "1":
                self.log_status(tbl + " - Total  (" + str(
                    loop_itr) + ") loop iterations  performed, still running... " + " Total rows deleted so far " + str(
                    rowcount) + "... Total rows to delete:" + str(rows_found))
            if rowcount >= rows_found or self.ROWS_AFFECTED == 0:
                break
        elapsed_time = time.time() - start_time
        if round(elapsed_time, 2) > self.long_op_time:
            self.log_status(
                "Time taken " + str(round(elapsed_time, 2)) + "s to delete " + tbl + " Loop itr " + str(loop_itr))
        return rowcount

    def summary(self):
        summary = []
        summary.append([self.bold("1) Database host server:"), self.bold(self.HOST)])
        summary.append([self.bold("2) Root Table:"), self.bold(self.MAIN_TABLE)])
        summary.append([self.bold("3) Where condition:"), self.bold(self.init_where)])
        table = PrettyTable(["Starting database archive script version 1.0:", self.VERSION])
        table.align = "l"
        table.header = False
        for t in summary:
            table.add_row([t[0], t[1]])
        self.log_status(table)

    def db_connect(self):
        try:
            self.src_db_cnx = mysql.connector.connect(user=self.USER, password=self.PASS, host=self.HOST,
                                                      port=self.MYSQL_PORT, connect_timeout=50000)
            self.src_db_cnx.raise_on_warnings = True
            self.src_db_cnx.autocommit = True

            cursor = self.src_db_cnx.cursor()
            if self.SET_VARS:
                session_vars = self.SET_VARS.split(';')
                for sv in session_vars:
                    if len(sv) > 1:
                        cursor.execute('SET SESSION ' + sv)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception, err:
            self.end_sub("Bad connection " + str(err))

    def next_batch(self):
        start_time = time.time()
        self.total_rows_affected = 0
        sql = "select column_name, 1 from information_Schema.columns where table_name='" + self.MAIN_TABLE + \
              "' AND column_key='PRI' AND table_schema='" + self.MAIN_DATABASE + "'"
        rs = self.run_sql(sql)
        for pk in rs:
            self.MAIN_PK_COL = (pk[0])
        sql = "SELECT IFNULL(GROUP_CONCAT(QUOTE(" + self.MAIN_PK_COL + ")),-11) ids, 1 FROM (SELECT " + \
              self.MAIN_PK_COL + " FROM " + self.MAIN_DATABASE + "." + self.MAIN_TABLE + " " + self.init_where + ") q"
        rs = self.run_sql(sql)

        for ids in rs:
            self.MAIN_TABLE_DATA = (ids[0])
        # Generate random batch id
        rnd_start = randint(1000, 10000)
        rnd_stop = randint(10000, 50000)
        self.BATCH_ID = randint(rnd_start, rnd_stop)
        elapsed_time = time.time() - start_time

    def run_sql(self, sql, connect_string=None):
        try:
            start_time = time.time()
            cnx = self.src_db_cnx
            cnx.raise_on_warnings = True

            cursor = cnx.cursor()
            # Execute main query passed to this function
            cursor.execute(sql)
            if cursor.with_rows:
                rs = cursor.fetchall()
            else:
                rs = []
            self.ROWS_AFFECTED = cursor.rowcount

            cursor.close()
            elapsed_time = time.time() - start_time
            return rs
        except (KeyboardInterrupt, SystemExit):
            if 'cnx' in locals():
                cnx.rollback()
            raise
        except Exception, err:
            self.end_sub(str(sql) + str(err))

    def get_primary_key(self, tblName):
        sql = "SELECT COLUMN_TYPE, COLUMN_NAME FROM `information_schema`.`COLUMNS` WHERE `TABLE_NAME` = '" + tblName + \
              "' AND COLUMN_KEY ='PRI' AND `TABLE_SCHEMA` = '" + self.MAIN_DATABASE + "'"
        rs = self.run_sql(sql)
        col = []
        sep = ","
        for row in rs:
            col.append(row[1])
        cols = sep.join(col)
        return cols

    # Get list of all child tables using recurrsion
    def validate_input(self, table, database):
        sql = "SELECT SCHEMA_NAME FROM  INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME='" + database + "'"
        resultset = self.run_sql(sql)
        if self.ROWS_AFFECTED == 0:
            self.end_sub(' Unknown database "' + database)
        sql = "SELECT TABLE_NAME FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='" + table + "'"
        resultset = self.run_sql(sql)
        if self.ROWS_AFFECTED == 0:
            self.end_sub(' Table "' + table + '" doesn\'t exist')
        if self.FILTER_LIST != "":
            excl_tbl_list = self.FILTER_LIST.split(',')
            for tbl in excl_tbl_list:
                sql = "SELECT TABLE_NAME FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='" + tbl + "'"
                resultset = self.run_sql(sql)
                if self.ROWS_AFFECTED == 0:
                    self.end_sub(' Table "' + tbl + '" doesn\'t exist')

    def get_child_tables(self, table):
        sql = "SELECT DISTINCT TABLE_NAME,COLUMN_NAME,REFERENCED_TABLE_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " \
              "WHERE REFERENCED_TABLE_NAME ='" + table + "' AND TABLE_SCHEMA='" + self.MAIN_DATABASE + "'"
        resultset = self.run_sql(sql)
        for result in resultset:
            self.sub_tables[self.child_table_ids] = (result[0], result[1], result[2])
            self.child_table_ids += 1
            self.get_child_tables(result[0])

    # Generate list of tables using [virtual] foreign keys  to archive
    def get_table_list(self, root_table=""):
        start_time = time.time()
        # exit function when nothing to archive
        if self.MAIN_TABLE_DATA.strip() == "-11":
            return
        loop_pass = 0
        parent_table_list = dict()
        parent_table_list[root_table] = self.MAIN_TABLE_DATA
        try:
            self.tables_list.clear()
            # Start with Root table, fetch list of all its child tables
            if root_table == "":
                root_table = self.MAIN_TABLE
            sql = "SELECT DISTINCT TABLE_NAME as tbl, COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE " \
                  "REFERENCED_TABLE_NAME ='" + root_table + "' AND TABLE_SCHEMA='" + self.MAIN_DATABASE + "';"
            rs01 = self.run_sql(sql)
            fk_list = []

            # Merge both virtual/physical foreign keys
            for row in rs01:
                fk_list.append([row[0], row[1], root_table])
            i = 1
            self.sub_tables = dict()
            for row in fk_list:
                ptbl = (row[0])
                pfield = (row[1])
                parent_table = row[2]
                # Exclude tables using ignore_tables list
                exclude_tbl = re.search(r'' + ptbl + '', self.FILTER_LIST, re.M | re.I)
                if exclude_tbl:
                    continue
                if parent_table == root_table:
                    self.tables_list[i] = (ptbl, parent_table, pfield, self.MAIN_TABLE_DATA)
                i += 1
                # for each table, fetch its all child tables
                self.sub_tables = dict()
                self.child_table_ids = 0
                self.get_child_tables(ptbl)
                # Generate list of tables - foreign key column and values

                for k in self.sub_tables.keys():
                    ctbl = self.sub_tables[k][0]
                    cfield = self.sub_tables[k][1]
                    ids_list = ""
                    parent_table = self.sub_tables[k][2]
                    r_col = ""
                    ids_cl = ""
                    for k, v in self.tables_list.items():
                        if v[0] == parent_table and v[2] == cfield:
                            ids_cl = v[3]
                            r_col = v[2]
                    if r_col == "":
                        for k, v in self.tables_list.items():
                            if v[0] == parent_table:
                                ids_cl = v[3]
                                r_col = v[2]

                    # Get values for foreign key using referencing table
                    pk = self.get_primary_key(parent_table)
                    sql = "SELECT IFNULL(GROUP_CONCAT(QUOTE(" + str(
                        pk) + ")),-11) ids, 1 FROM " + self.MAIN_DATABASE + "." + parent_table + " WHERE " + str(
                        r_col) + " IN(" + str(ids_cl) + ")"
                    batch_summary_display = PrettyTable(["Table", "Column", "Parent Table", "#"])
                    batch_summary_display.align = "l"
                    batch_summary_display.header = False
                    for k, v in self.tables_list.items():
                        batch_summary_display.add_row([v[0], v[1], v[2], v[3]])

                    rs = self.run_sql(sql)
                    for ids in rs:
                        ids_list = ids[0]
                    # Exclude tables using ignore_tables list

                    exclude_tbl = re.search(r'' + ctbl + '', self.FILTER_LIST, re.M | re.I)
                    if exclude_tbl:
                        continue
                    # Add to associate array - child/parent table, foreign key and list of values
                    self.tables_list[i] = (ctbl, parent_table, cfield, ids_list)
                    i += 1
            # Dispaly stats [debugging only]
            batch_summary_display = PrettyTable(["#", "Table", "Parent Table", "ID_column", "values"])
            batch_summary_display.align = "l"
            batch_summary_display.header = False
            for k, v in self.tables_list.items():
                batch_summary_display.add_row([k, v[0], v[1], v[2], v[3][0:50]])
            elapsed_time = time.time() - start_time
            if round(elapsed_time, 2) >= int(self.long_op_time) and self.VERBOSE == "1":
                self.log_status('Time spent to prepare tables list:' + str(int(elapsed_time)) + 's')
            # self.end_sub('')
        except Exception, err:
            print("Error:" + str(err))
            raise


parser = argparse.ArgumentParser(description='Delete hierarchical data ')

parser.add_argument('--host', dest='host', type=str,
                    help='hostname, (default: %(default)s)', default="localhost")

parser.add_argument('--user', dest='user', type=str,
                    help='database user, (default: %(default)s)', default="root")

parser.add_argument('--password', dest='password', type=str, required=True, action="store",
                    help='database password')

parser.add_argument('--database', dest='database', required=True, type=str,
                    help='database name')

parser.add_argument('--limit', dest='limit', type=int,
                    help='batch size, (default: %(default)s)', default=100)

parser.add_argument('--dry-run', dest='dry_run', action='store_true',
                    help='print queries and do nothing')
parser.add_argument('--port', dest='port', type=str, default="3306",
                    help='Port number to use for connection, (default: %(default)s) ')

args = parser.parse_args()

value = ""

def d_range(start, stop, step):
    while start < stop:
        yield start
        start += step


for i in d_range(start_from, end_at, 1):
    j = '%03d' % i
    k = "".join((prefix, j))
    l = "".join(("\'", "".join((k, "\'"))))
    value = ",".join((value, l))

value = value[1:]

args.where = str("DEVICE_IDENTIFICATION IN (" + value + ")")

args.root_table = "DM_DEVICE"

archive = DBArchive()
archive.DRY_RUN = None
archive.MYSQL_PORT = 3306
archive.SET_VARS = "group_concat_max_len=5*1024*1024"
archive.SLEEP_TIME = 0
archive.VERBOSE = "1"
archive.FILTER_LIST = ""
archive.TMP_DIR = '/tmp'

if args.host:
    archive.HOST = args.host
if args.user:
    archive.USER = args.user
if args.password:
    archive.PASS = args.password
if args.database:
    archive.MAIN_DATABASE = args.database
if args.root_table:
    archive.MAIN_TABLE = args.root_table
if args.where:
    archive.init_where = "WHERE " + args.where
if args.port:
    archive.MYSQL_PORT = args.port
if args.limit:
    archive.BATCH_SIZE = args.limit
    archive.init_where = archive.init_where + " LIMIT " + str(archive.BATCH_SIZE)

for v in sys.argv:
    if str(v) == "--dry-run":
        archive.DRY_RUN = "true"
        print Color.CYAN + "DRY-RUN" + Color.END

if len(vars(args)) < 4:
    print str(args)
    parser.print_help()

try:
    archive.VERSION = "1.0"
    archive.CAPTION = 'del_data'
    archive.summary()
    archive.db_connect()
    archive.validate_input(archive.MAIN_TABLE, archive.MAIN_DATABASE)
    original_time = float(archive.SLEEP_TIME)
    batch_size = archive.BATCH_SIZE
    init_where = archive.init_where
    main_table = archive.MAIN_TABLE
    archive.is_primary_tbl_list = 1
    archive.show_stats = 1
except KeyboardInterrupt:
    print Color.RED + "\n Ctrl+c detected \n Bye" + Color.END
    sys.exit()

archive.log_status("Running first batch")
rows_deleted = 1
while rows_deleted:
    try:
        start_time = time.time()
        archive.next_batch()
        archive.get_table_list()
        rows_deleted = archive.start_archive()
        elapsed_time = time.time() - start_time
        archive.log_status(Color.GREEN + str(time.ctime()) + ' Batch completed. Execution time: ' + str(
            int(elapsed_time)) + 's - Items deleted :' + str(rows_deleted) + Color.END)
        if archive.DRY_RUN:
            archive.end_sub(' DRY RUN end')
        if rows_deleted:
            archive.log_status('Taking a nap:' + str(archive.SLEEP_TIME) + "s")
        else:
            sys.exit()
        time.sleep(float(archive.SLEEP_TIME))
        archive.log_status(archive.bold("Running next batch"))
        if archive.DRY_RUN:
            archive.end_sub('dry_run end')
    except KeyboardInterrupt:
        archive.end_sub(Color.RED + "\n Ctrl+c detected \n Bye" + Color.END)
        sys.exit()
    except SystemExit:
        print Color.RED + "\n Program ended \n Bye" + Color.END
        sys.exit()
