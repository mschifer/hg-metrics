import sqlite3
import sys
import traceback


METRICS_RELEASE_TABLE_NAME    = 'metrics_releases'
METRICS_FILE_LIST_TABLE_NAME  = 'metrics_files'
METRICS_CHANGES_TABLE_NAME    = 'metrics_changes'
METRICS_SUMMARY_TABLE_NAME    = 'metrics_summary'
METRICS_COMMITTERS_TABLE_NAME = 'metrics_committers'

# TODO Should really fully parameterize table names...future.

# Create table statements
create_table_stmts = {METRICS_RELEASE_TABLE_NAME: '''CREATE TABLE metrics_releases(release_name VARCHAR(100), 
                                           release_id INTEGER PRIMARY KEY)''',
                      METRICS_FILE_LIST_TABLE_NAME: '''CREATE TABLE metrics_files(file_name VARCHAR(250),
                                            file_id INTEGER PRIMARY KEY, mean INTEGER, stdev INTEGER)''',
                      METRICS_CHANGES_TABLE_NAME: '''CREATE TABLE metrics_changes (delta INTEGER, total_lines INTEGER, percent_change INTEGER,
                                          file_id INTEGER, release_id INTEGER, bug VARCHAR(8), commit_id VARCHAR(100), is_backout INTEGER,
                                          committer_name VARCHAR(25), reviewer VARCHAR(25), approver VARCHAR(25), msg VARCHAR(250), is_regression INTEGER, found DATE, fixed DATE)''',
                      METRICS_SUMMARY_TABLE_NAME: '''CREATE TABLE metrics_summary (release_id INTEGER, file_id INTEGER, percent_change INTEGER, bugs VARCHAR(100), backout_count INTEGER, committers VARCHAR(250), reviewers VARCHAR(250), approvers VARCHAR(250), msgs VARCHAR(500), total_commits INTEGER, bug_count INTEGER, regression_count INTEGER, author_count INTEGER)''',
                      METRICS_COMMITTERS_TABLE_NAME: '''CREATE TABLE metrics_committers (bzemail VARCHAR(25), email VARCHAR(25), manager_email VARCHAR(25), department VARCHAR(50))'''}

# Does the table exist query
TABLE_EXIST_STMT = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"

# Insert statements
INSERT_FILE_NAME  = 'INSERT INTO metrics_files (file_name) VALUES(?)'
INSERT_RELEASE    = 'INSERT INTO metrics_releases (release_name) VALUES (?)'
INSERT_CHANGES    = 'INSERT INTO metrics_changes (delta, total_lines, percent_change, file_id, release_id, bug, commit_id, is_backout, committer_name, reviewer, approver, msg, is_regression, found, fixed) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
INSERT_SUMMARY    = 'INSERT INTO metrics_summary (release_id, bugs, file_id, percent_change, backout_count, committers, reviewers, approvers, msgs, total_commits, bug_count, regression_count, author_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'
INSERT_COMMITTERS = 'INSERT INTO metrics_committers (bzemail, email, manager_email, department) VALUES (?,?,?,?)"

# Update statements
UPDATE_AVG_CHANGE      = 'UPDATE metrics_files SET  mean = ?, stdev = ?  WHERE file_id = ?'
UPDATE_PERCENT_CHANGE  = 'UPDATE metrics_changes SET percent_change= ?  WHERE file_id = ? and commit_id = ?'
UPDATE_REGRESSION_CHANGE  = 'UPDATE metrics_changes SET is_regression = ?, found = ?, fixed = ? WHERE bug = ?'
UPDATE_SUMMARY         = 'UPDATE metrics_summary SET percent_change = ?, bugs = ?, backout_count = ?, committers = ?, reviewers = ?, approvers = ?, msgs = ?, total_commits = ?, bug_count = ?, regression_count = ?, author_count = ? WHERE release_id = ? AND file_id = ?'
UPDATE_REGRESSION_COUNT_SUMMARY  = 'UPDATE metrics_summary SET regression_count = ?  WHERE  file_id = ? AND release_id = ?'
UPDATE_BUG_COUNT_SUMMARY  = 'UPDATE metrics_summary SET bug_count = ?  WHERE  bug = ?'
UPDATE_AUTHOR_COUNT_SUMMARY  = 'UPDATE metrics_summary SET author_count = ?  WHERE  bug = ?'
UPDATE_COMMITTERS = 'UPDATE metrics_committers SET bzemail = ?, SET email = ?, SET manager_email = ?, SET department = ?'

# Could not get this to parameterize properly with ?'s
DROP_TABLE_STMT = "DROP TABLE %s"

# Query data statements
GET_ALL_CHANGE_PER_FILES = 'SELECT metrics_files.file_name, metrics_releases.release_name, metrics_changes.percent_change ' \
                      'FROM metrics_files, metrics_releases, metrics_changes ' \
                      'WHERE metrics_files.file_id = metrics_changes.file_id and metrics_releases.release_id = metrics_changes.release_Id ' \
                      'ORDER BY  metrics_changes.file_id,metrics_changes.release_id'

GET_CHANGE_PER_FILE = 'SELECT metrics_files.file_name, metrics_releases.release_name, ' \
                      'metrics_changes.release_id, metrics_changes.percent_change,' \
                      'metrics_changes.delta, metrics_changes.total_lines ' \
                      'FROM metrics_files, metrics_releases, metrics_changes ' \
                      'WHERE metrics_files.file_id = metrics_changes.file_id ' \
                      'AND metrics_releases.release_id = metrics_changes.release_Id ' \
                      'AND metrics_changes.file_id =  ? ' \
                      'ORDER BY metrics_changes.release_id'
GET_RELEASE_ID = 'SELECT release_id FROM metrics_releases WHERE release_name = ?' 
GET_FILE_ID    = 'SELECT file_id FROM metrics_files WHERE file_name = ?' 
GET_COMMIT_ID  = 'SELECT commit_id from metrics_changes where commit_id = ? AND file_id = ?'
GET_RELEASES   = 'SELECT release_id, release_name FROM metrics_releases ORDER BY release_id'
GET_RELEASE_IDS = 'SELECT release_id FROM metrics_releases ORDER BY release_id'
GET_FILES      = 'SELECT file_id, file_name FROM metrics_files ORDER BY file_id'
GET_FILE_IDS   = 'SELECT file_id FROM metrics_files ORDER BY file_id'
GET_CHANGE_DATA= 'SELECT total_lines, delta, percent_change, file_id, release_id, bug, commit_id, is_backout, committer_name, reviewer, approver, msg ' \
                 'FROM metrics_changes WHERE file_id = ?'
GET_CHANGE_PER_FILE_RELEASE  = 'SELECT file_id, total_lines, delta, percent_change, commit_id, bug, is_backout, committer_name, reviewer, approver, msg, is_regression FROM metrics_changes ' \
                     'WHERE file_id = ? AND release_id = ? '
                 
GET_SUMMARY_DATA = 'SELECT release_id, file_id, percent_change FROM metrics_summary WHERE release_id = ? AND file_id = ?'
GET_ALL_SUMMARY_DATA = 'SELECT release_id, file_id, percent_change FROM metrics_summary WHERE file_id = ?'
GET_FILES_PER_RELEASE = 'SELECT file_id FROM metrics_changes WHERE release_id =? ORDER BY file_id'
GET_BUGS = 'SELECT DISTINCT bug FROM metrics_changes WHERE bug > 0'
GET_BUGS_BY_FILE = 'SELECT DISTINCT bug FROM metrics_changes WHERE file_id = ? AND bug > 0'
GET_BUGS_BY_FILE_RELEASE  = 'SELECT DISTINCT bug FROM metrics_changes WHERE file_id = ? AND release_id = ? AND bug > 0'
GET_REGRESSIONS_BY_FILE_RELEASE  = 'SELECT DISTINCT bug, is_regression FROM metrics_changes WHERE file_id = ? AND release_id = ? AND is_regression = 1'
GET_REGRESSIONS_BY_RELEASE  = 'SELECT DISTINCT bug, is_regression FROM metrics_changes WHERE release_id = ? AND is_regression = 1'
GET_REGRESSIONS = 'SELECT DISTINCT bug, file_id, release_id FROM metrics_changes WHERE is_regression = 1 ORDER BY file_id, release_id'
#GET_REGRESSIONS_BY_FILE_RELEASE  = 'SELECT DISTINCT bug, sum(is_regression) FROM metrics_changes WHERE file_id = ? AND release_id = ?'
GET_ALL_COMMITTERS = 'SELECT bzemail, email, manager_email, department FROM metrics_committers'
GET_COMMITTER_EMAIL = 'SELECT bzemail, email, manager_email, department FROM metrics_committers WHERE email = ?'
GET_COMMITTER_BZEMAIL = 'SELECT bzemail, email, manager_email, department FROM metrics_committers WHERE bzemail = ?'


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
                print query
                print queryparams
                #print '--'
                cursor.execute(query, queryparams)
                print 'TOTAL CHANGES:', self._dbconn.total_changes;
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
    def add_change_values(self,file_id,release_id, delta, total_lines, percent_change, bug, commit_id, is_backout, committer_name, reviewer, approver, msg, is_regression):
        # Add entry to the METRICS_CHANGES table
        c = self._dbconn.cursor()
        self._run_execute(c, INSERT_CHANGES , [delta, total_lines, percent_change, file_id, release_id, bug, commit_id, is_backout, committer_name, reviewer, approver, msg, is_regression])
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
        return c.fetchone()

    def get_commit_id(self, commit_id, file_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_COMMIT_ID, [commit_id, file_id])
        return c.fetchall()

    def get_release_id(self, release_name):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_RELEASE_ID, [release_name])
        return c.fetchall()

    def get_files(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_FILES)
        return c.fetchall()

    def get_file_ids(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_FILE_IDS)
        return c.fetchall()

    def get_releases(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_RELEASES)
        return c.fetchall()

    def get_release_ids(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_RELEASE_IDS)
        return c.fetchall()

    def get_changes_by_file(self, file_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_CHANGE_PER_FILE, [file_id])
        return c.fetchall()

    def get_changes_by_file_release(self, file_id, release_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_CHANGE_PER_FILE_RELEASE, [file_id, release_id])
        return c.fetchall()

    def update_avg_change(self, file_id, mean, stdev):
        c = self._dbconn.cursor()
        c = self._run_execute(c, UPDATE_AVG_CHANGE, (mean, stdev, file_id))
        self._dbconn.commit()

    def get_file_changes(self,file_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_CHANGE_DATA, [file_id] )
        return c.fetchall()

    def update_percent_change(self, percent_change, file_id, commit_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c, UPDATE_PERCENT_CHANGE, (percent_change, file_id, commit_id))
        self._dbconn.commit()

    def add_summary_data(self, release_id, file_id, percent_change, bugs, backout_count, committers, reviewers, approvers, msgs, total_commits, bug_count, regression_count, author_count):
        c = self._dbconn.cursor()
        c = self._run_execute(c, INSERT_SUMMARY, (release_id, bugs, file_id, percent_change, backout_count, committers, reviewers, approvers, msgs, total_commits, bug_count, regression_count, author_count ))
        self._dbconn.commit()

    def update_summary_data(self, release_id, file_id, percent_change, bugs, backout_count, committers, reviewers, approvers, msgs, total_commits, bug_count, regression_count, author_count):
        c = self._dbconn.cursor()
        c = self._run_execute(c, UPDATE_SUMMARY, (percent_change, bugs, backout_count, committers, reviewers, approvers, msgs, total_commits, bug_count, regression_count, author_count, release_id, file_id))
        self._dbconn.commit()

    def get_summary_data(self, release_id, file_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_SUMMARY_DATA, [release_id, file_id] )
        return c.fetchone()

    def get_all_summary_data(self, file_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_ALL_SUMMARY_DATA, [file_id] )
        return c.fetchall()

    def get_files_per_release(self, release_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_FILES_PER_RELEASE, [release_id] )
        return c.fetchall()
   
    def get_bugs(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_BUGS)
        return c.fetchall()

    def get_bugs_by_file(self, file_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_BUGS, [file_id])
        return c.fetchall()

    def get_bugs_by_file_release(self, file_id, release_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_BUGS, [file_id, release_id])
        return c.fetchall()

    def update_regression_change(self, is_regression, found, fixed, bug ):
        c = self._dbconn.cursor()
        c = self._run_execute(c, UPDATE_REGRESSION_CHANGE , (is_regression, found, fixed, bug))
        self._dbconn.commit()

    def get_regressions_by_file_release(self, file_id, release_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_REGRESSIONS_BY_FILE_RELEASE, (file_id, release_id))
        return c.fetchall()

    def get_regressions_by_release(self, release_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_REGRESSIONS_BY_RELEASE, (release_id))
        return c.fetchall()

    def get_regressions(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_REGRESSIONS)
        return c.fetchall()

    def update_regression_count(self, file_id, release_id, regression_count):
        c = self._dbconn.cursor()
        c = self._run_execute(c, UPDATE_REGRESSION_COUNT_SUMMARY, (regression_count, file_id, release_id))
        self._dbconn.commit()

    def raw_fetch_sql(self, sql):
        c = self._dbconn.cursor()
        c = self._run_execute(c, sql)
        return c.fetchall()

    def raw_commit_sql(self, sql):
        c = self._dbconn.cursor()
        c = self._run_execute(c, sql)
        self._dbconn.commit()

    def add_committer(self, bzemail, email, manager_email, deptname):
        c = self._dbconn.cursor()
        c = self._run_execute(c, INSERT_COMMITTERS, (bzemail, email, manager_email, deptname))
        self._dbconn.commit()

    def update_committer(self, bzemail, email, manager_email, deptname):
        c = self._dbconn.cursor()
        c = self._run_execute(c, INSERT_COMMITTERS, (bzemail, email, manager_email, deptname))
        self._dbconn.commit()

   def get_all_committers(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_ALL_COMMITTERS)
        return c.fetchall()

   def get_all_committer(self, email):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_COMMITTER)
        return c.fetchall()

