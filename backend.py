import sqlite3
import sys
import traceback


METRICS_RELEASE_TABLE_NAME = 'metrics_releases'
METRICS_FILE_LIST          = 'metrics_files'
METRICS_CHANGES            = 'metrics_changes'

# TODO Should really fully parameterize table names...future.

# Create table statements
create_table_stmts = {METRICS_RELEASE_TABLE_NAME: '''CREATE TABLE metrics_releases(release_name VARCHAR(100), 
                                           release_id INTEGER PRIMARY KEY)''',
                      METRICS_FILE_LIST: '''CREATE TABLE metrics_files(file_name VARCHAR(250),
                                            file_id INTEGER PRIMARY KEY)''',
                      METRICS_CHANGES: '''CREATE TABLE metrics_changes(delta INTEGER, total_lines INTEGER, percent_change INTEGER,
                                          file_id INTEGER, release_id INTEGER) ''' }
# Does the table exist query
TABLE_EXIST_STMT = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"

# Insert statements
INSERT_FILE_NAME = 'INSERT INTO metrics_files (file_name) VALUES(?)'
INSERT_RELEASE   = 'INSERT INTO metrics_releases (release_name) VALUES (?)'
INSERT_CHANGES   = 'INSERT INTO metrics_changes (delta, total_lines, percent_change, file_id, release_id) VALUES (?,?,?,?,?)'

# Could not get this to parameterize properly with ?'s
DROP_TABLE_STMT = "DROP TABLE %s"

# Query data statements
GET_ALL_CHANGE_PER_FILES = 'SELECT metrics_files.file_name, metrics_releases.release_name, metrics_changes.percent_change ' \
                      'FROM metrics_files, metrics_releases, metrics_changes ' \
                      'WHERE metrics_files.file_id = metrics_changes.file_id and metrics_releases.release_id = metrics_changes.release_Id ' \
                      'ORDER BY  metrics_changes.file_id,metrics_changes.release_id'

GET_CHANGE_PER_FILE = 'SELECT metrics_files.file_name, metrics_releases.release_name, metrics_changes.percent_change ' \
                      'FROM metrics_files, metrics_releases, metrics_changes ' \
                      'WHERE metrics_files.file_id = metrics_changes.file_id AND metrics_releases.release_id = metrics_changes.release_Id AND metrics_changes.file_id =  ? ' \
                      'ORDER BY metrics_changes.release_id'
GET_RELEASE_ID = 'SELECT release_id FROM metrics_releases WHERE release_name = ?' 
GET_FILE_ID    = 'SELECT file_id FROM metrics_files WHERE file_name = ?' 

GET_RELEASES = 'SELECT release_id, release_name FROM metrics_releases ORDER BY release_id'
GET_FILES    = 'SELECT file_id, file_name FROM metrics_files ORDER BY file_id'

class SQLiteBackend(object):

    ###
    # Establish connection to database
    def __init__(self, dbname='churndb.sql'):
        if dbname:
            self._dbconn = sqlite3.connect(dbname)
        else:
            self._dbconn = sqlite3.connect('churndb.sql')
        self._verify_tables()

    ### 
    # Execute SQL statements and return cursor to data
    def _run_execute(self, cursor, query, queryparams = None):
        # Just a utility to keep from having to type try...finallies everywhere
        # Note that queryparams is expected to be a list
        try:
            if cursor == None:
                print "WARNING: Getting new cursor"
                cursor = self._dbconn.cursor()
            if queryparams:
                cursor.execute(query, queryparams)
            else:
                cursor.execute(query)
        except:
            print "Exception during query: %s" % query
            if queryparams:
                print "Query Params for this query: %s" % queryparams
            traceback.print_exc()
            cursor.close()
            cursor = None

        return cursor

    ###
    #  Utility Functions
    #  Verify/Create Tables
    #  Drop Tables
    #  
    def _verify_tables(self):
        # Verify that tables exist in our database and create them if not
        c = self._dbconn.cursor()
        for t in create_table_stmts:
            if not self._table_exists(t):
                self._run_execute(c, create_table_stmts[t])

    def _drop_tables(self):
        c = self._dbconn.cursor()
        for t in create_table_stmts:
            if self._table_exists(t):
                self._run_execute(c, DROP_TABLE_STMT % t)
        self._dbconn.commit()

    def _table_exists(self, name):
        c = self._dbconn.cursor()
        c = self._run_execute(c, TABLE_EXIST_STMT, [name])
        r = c.fetchone()
        return (r and (r[0] == name))

    ###
    # Data Functions
    # Add data to tables
    # Get Data from tables
    def add_change_values(self,file_id,release_id, delta, total_lines, percent_change):
        # Add entry to the METRICS_CHANGES table
        c = self._dbconn.cursor()
        self._run_execute(c, INSERT_CHANGES , [delta, total_lines, percent_change, file_id, release_id])
        self._dbconn.commit()
 
    def add_file_values(self, file_name):
        # Add entry to the METRICS_FILES table
        c = self._dbconn.cursor()
        self._run_execute(c, INSERT_FILE_NAME, [file_name])
        self._dbconn.commit()
        return self.get_file_id(file_name)

    def add_release_values(self, release_name):
        # Add entry to the METRICS_FILES table
        c = self._dbconn.cursor()
        self._run_execute(c, INSERT_RELEASE, [release_name])
        self._dbconn.commit()
        return self.get_release_id(release_name)

    def get_file_id(self, file_name):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_FILE_ID, [file_name])
        return c.fetchall()

    def get_release_id(self, release_name):
        print 'Get Release ID: %s' % release_name
        print  GET_RELEASE_ID, [release_name]
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_RELEASE_ID, [release_name])
        return c.fetchall()

    def get_files(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_FILES)
        return c.fetchall()

    def get_releases(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_RELEASES)
        return c.fetchall()

    def get_changes_by_file(self, file_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_CHANGE_PER_FILE, [file_id])
        return c.fetchall()
