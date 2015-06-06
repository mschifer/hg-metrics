import sqlite3
import sys
import traceback
import pprint


METRICS_RELEASE_TABLE_NAME    = 'metrics_releases'
METRICS_FILE_LIST_TABLE_NAME  = 'metrics_files'
METRICS_CHANGES_TABLE_NAME    = 'metrics_changes'
METRICS_SUMMARY_TABLE_NAME    = 'metrics_summary'
METRICS_PEOPLE_TABLE_NAME     = 'metrics_people'
METRICS_REGRESSIONS_TABLE_NAME= 'metrics_bugs'
METRICS_TEAM_VIEW             = 'metrics_team_view'
METRICS_RELEASE_MASTER_VIEW   = 'metrics_release_master_view'
METRICS_FILE_REGRESSION_RATE_VIEW = 'metrics_file_regression_rate_view'
METRICS_TEAM_REGRESSION_RATE_VIEW = 'metrics_team_regression_rate_view'
METRICS_REGRESSION_COUNT_VIEW = 'metrics_regression_count_view'
METRICS_BUG_COUNT_VIEW = 'metrics_bug_count_view'
METRICS_REGRESSIONS_FIXED_VIEW = 'metrics_regressions_fixed_view'
METRICS_BUGS_FIXED_VIEW = 'metrics_bugs_fixed_view'
METRICS_BACKOUT_COUNT_VIEW = 'metrics_backout_count_view'
METRICS_BUG_STATS_VIEW = 'metrics_bug_stats_view'




# TODO Should really fully parameterize table names...future.

# Create table statements
create_table_stmts = {METRICS_RELEASE_TABLE_NAME: '''CREATE TABLE metrics_releases(release_name VARCHAR(100), 
                         release_number INTEGER, release_id INTEGER PRIMARY KEY)''',
                      METRICS_FILE_LIST_TABLE_NAME: '''CREATE TABLE metrics_files(file_name VARCHAR(250),
                         file_id INTEGER PRIMARY KEY, mean INTEGER, stdev INTEGER)''',
                      METRICS_CHANGES_TABLE_NAME: '''CREATE TABLE metrics_changes (delta INTEGER, 
                         total_lines INTEGER, percent_change INTEGER, file_id INTEGER, release_id INTEGER, 
                         bug VARCHAR(8), commit_id VARCHAR(100), is_backout INTEGER, committer_name VARCHAR(25), 
                         reviewer VARCHAR(25), approver VARCHAR(25), msg VARCHAR(250), is_regression INTEGER, 
                         found DATE, fixed DATE)''',
                      METRICS_SUMMARY_TABLE_NAME: '''CREATE TABLE metrics_summary (release_id INTEGER, 
                          file_id INTEGER, percent_change INTEGER, bugs VARCHAR(100), backout_count INTEGER, 
                          committers VARCHAR(250), reviewers VARCHAR(250), approvers VARCHAR(250), msgs VARCHAR(500), 
                          total_commits INTEGER, bug_count INTEGER, regression_count INTEGER, author_count INTEGER)''',
                      METRICS_PEOPLE_TABLE_NAME: '''CREATE TABLE metrics_people (people_id INTEGER PRIMARY KEY,
                          name VARCHAR(25), bzemail VARCHAR(25), email VARCHAR(25), manager_email VARCHAR(25), 
                          manager_id INTEGER,  department VARCHAR(50))''',
                      METRICS_REGRESSIONS_TABLE_NAME: '''CREATE TABLE metrics_bugs (bug INTEGER PRIMARY KEY, 
                          version VARCHAR(12), found DATE, fixed DATE, product VARCHAR(25), status VARCHAR(25), 
                          component VARCHAR(25), is_regression INTEGER, release_id INTEGER)'''
               
}

create_view_stmts1 = {
                      METRICS_TEAM_VIEW: '''CREATE VIEW metrics_team_view AS SELECT m.name AS manager, 
                          m.bzemail AS manageremail, m.people_id AS m_id, e.name, e.bzemail AS committer, 
                          e.people_id AS c_id, e.department AS department FROM metrics_people e 
                          INNER JOIN metrics_people m ON e.manager_id = m.people_id ORDER BY m_id''',
                      METRICS_FILE_REGRESSION_RATE_VIEW: '''CREATE VIEW metrics_file_regression_rate_view 
                          AS SELECT mc.file_id, mf.file_name, TOTAL(mc.delta) AS lines_changed, 
                          TOTAL(is_regression) AS regressions, TOTAL(is_backout) AS backouts 
                          FROM metrics_files mf, metrics_changes mc WHERE mf.file_id = mc.file_id  
                          GROUP BY mc.file_id''',
                      METRICS_RELEASE_MASTER_VIEW: '''CREATE VIEW metrics_release_master_view 
                          AS SELECT mr.release_id, mr.release_name, mr.release_number, ms.start_date 
                          FROM metrics_releases mr, metrics_release_schedule ms 
                          WHERE (ms.nightly = mr.release_number and mr.release_name like 'nightly%') 
                          OR (ms.beta = mr.release_number and mr.release_name like 'beta%') 
                          OR (ms.release = mr.release_number and mr.release_name like 'release%') 
                          OR (ms.aurora = mr.release_number and mr.release_name like 'aurora%') ''',
                      METRICS_TEAM_REGRESSION_RATE_VIEW:'''CREATE VIEW metrics_team_regression_rate_view 
                          AS SELECT m.manager, m.department, r.release_number, TOTAL(c.delta) AS lines_changed, 
                          TOTAL(is_regression) AS regressions, TOTAL(is_backout) AS backouts 
                          FROM metrics_team_view m, metrics_changes c, metrics_releases r 
                          WHERE c.committer_name = m.name AND c.release_id = r.release_id 
                          GROUP BY c.committer_name, r.release_number''',
                      METRICS_REGRESSION_COUNT_VIEW:'''CREATE VIEW metrics_regression_count_view 
                          AS SELECT count(*) AS regression_count, release_id 
                          FROM metrics_bugs WHERE is_regression > 0 GROUP BY release_id''',
                      METRICS_BUG_COUNT_VIEW: '''CREATE VIEW metrics_bug_count_view 
                          AS SELECT count(*) AS bug_count, release_id 
                          FROM metrics_bugs GROUP BY release_id''',
                      METRICS_REGRESSIONS_FIXED_VIEW: '''CREATE VIEW metrics_regressions_fixed_view 
                          AS SELECT count(*) AS regressions_fixed, release_id 
                          FROM metrics_bugs WHERE is_regression > 0 and fixed > "2000-01-01" GROUP BY release_id''',
                      METRICS_BUGS_FIXED_VIEW: '''CREATE VIEW metrics_bugs_fixed_view 
                          AS SELECT count(*) AS bugs_fixed, release_id 
                          FROM metrics_bugs WHERE fixed > "2000-01-01" GROUP BY release_id''',
                      METRICS_BACKOUT_COUNT_VIEW: '''CREATE VIEW metrics_backout_count_view 
                          AS SELECT count(*) AS backout_count, release_id 
                          FROM metrics_changes WHERE is_backout = 1 GROUP BY release_id'''
}
create_view_stmts2 = {
                      METRICS_BUG_STATS_VIEW: '''CREATE VIEW metrics_bug_stats_view 
                          AS SELECT rcv.regression_count, bcv.bug_count, rfv.regressions_fixed, 
                          bfv.bugs_fixed, bocv.backout_count, rcv.release_id 
                          FROM metrics_regression_count_view rcv, metrics_bug_count_view bcv, metrics_regressions_fixed_view rfv, 
                          metrics_bugs_fixed_view bfv, metrics_backout_count_view bocv 
                          WHERE rcv.release_id =  bcv.release_id 
                          AND rcv.release_id =  rfv.release_id 
                          AND rcv.release_id = bfv.release_id 
                          AND rcv.release_id =  bocv.release_id 
                          ORDER BY rcv.release_id'''
}

# Does the table exist query
TABLE_EXIST_STMT = 'SELECT name FROM sqlite_master WHERE type="table" AND name=?'
VIEW_EXIST_STMT  = 'SELECT name FROM sqlite_master WHERE type="view" AND name=?'

# Insert statements
INSERT_FILE_NAME  = 'INSERT INTO metrics_files (file_name) VALUES(?)'
INSERT_RELEASE    = 'INSERT INTO metrics_releases (release_name, release_number) VALUES (?,?)'

INSERT_CHANGES    = 'INSERT INTO metrics_changes (file_id, release_id, delta, total_lines, percent_change, ' \
                    'bug, commit_id, is_backout, committer_name, reviewer, approver, msg, is_regression, ' \
                    'found, fixed) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

INSERT_SUMMARY    = 'INSERT INTO metrics_summary (release_id, bugs, file_id, percent_change, backout_count, ' \
                    'committers, reviewers, approvers, msgs, total_commits, bug_count, regression_count, author_count) ' \
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'
INSERT_PEOPLE = 'INSERT INTO metrics_people (name, bzemail, email, manager_email, department) VALUES (?,?,?,?,?)'
INSERT_REGRESSION = 'INSERT INTO metrics_bugs (bug, version, found, fixed, product, status, component, ' \
                     'is_regression, release_id) VALUES (?,?,?,?,?,?,?,?,?)'

# Update statements
UPDATE_AVG_CHANGE         = 'UPDATE metrics_files SET  mean = ?, stdev = ?  WHERE file_id = ?'
UPDATE_PERCENT_CHANGE     = 'UPDATE metrics_changes SET percent_change= ?  WHERE file_id = ? and commit_id = ?'
UPDATE_REGRESSION_CHANGE  = 'UPDATE metrics_changes SET is_regression = ?, found = ?, fixed = ? WHERE bug = ?'
UPDATE_SUMMARY            = 'UPDATE metrics_summary SET percent_change = ?, bugs = ?, backout_count = ?, ' \
                            'committers = ?, reviewers = ?, approvers = ?, msgs = ?, total_commits = ?,  ' \
                            'bug_count = ?, regression_count = ?, author_count = ? WHERE release_id = ? AND file_id = ?'
UPDATE_REGRESSION_COUNT_SUMMARY  = 'UPDATE metrics_summary SET regression_count = ?  WHERE  file_id = ? AND release_id = ?'
UPDATE_BUG_COUNT_SUMMARY  = 'UPDATE metrics_summary SET bug_count = ?  WHERE  bug = ?'
UPDATE_AUTHOR_COUNT_SUMMARY  = 'UPDATE metrics_summary SET author_count = ?  WHERE  bug = ?'
UPDATE_PEOPLE = 'UPDATE metrics_people SET bzemail = ?, SET email = ?, SET manager_email = ?, SET department = ? WHERE bzemail = ?'
UPDATE_REGRESSIONS = 'UPDATE metrics_bugs SET fixed = ?, status = ?,is_regression = ?, release_id = ?  WHERE bug = ?'
UPDATE_BUG_FIXED_TIME = 'UPDATE metrics_bugs set time_to_fix = ? WHERE bug = ?'
UPDATE_BUG_RELEASE_ID = 'UPDATE metrics_bugs SET release_id = ? WHERE bug = ?'
UPDATE_MANAGER_ID     = 'UPDATE metrics_people SET manager_id = ?  WHERE manager_email = ?' 


# Could not get this to parameterize properly with ?'s
DROP_TABLE_STMT = "DROP TABLE %s"
DROP_VIEW_STMT = "DROP VIEW %s"

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
GET_CHANGE_DATA= 'SELECT total_lines, delta, percent_change, file_id, release_id, bug, ' \
                 'commit_id, is_backout, committer_name, reviewer, approver, msg ' \
                 'FROM metrics_changes WHERE file_id = ?'
GET_CHANGE_PER_FILE_RELEASE  = 'SELECT file_id, total_lines, delta, percent_change, commit_id, ' \
                 'bug, is_backout, committer_name, reviewer, approver, msg, is_regression FROM metrics_changes ' \
                 'WHERE file_id = ? AND release_id = ? '
                 
GET_SUMMARY_DATA = 'SELECT release_id, file_id, percent_change FROM metrics_summary WHERE release_id = ? AND file_id = ?'
GET_ALL_SUMMARY_DATA = 'SELECT release_id, file_id, percent_change FROM metrics_summary WHERE file_id = ?'
GET_FILES_PER_RELEASE = 'SELECT file_id FROM metrics_changes WHERE release_id =? ORDER BY file_id'
GET_BUGS = 'SELECT DISTINCT bug FROM metrics_changes WHERE bug > 0'
GET_BUGS_BY_FILE = 'SELECT DISTINCT bug FROM metrics_changes WHERE file_id = ? AND bug > 0'
GET_BUGS_BY_FILE_RELEASE = 'SELECT DISTINCT bug FROM metrics_changes WHERE file_id = ? AND release_id = ? AND bug > 0'
GET_REGRESSIONS_BY_FILE_RELEASE = 'SELECT DISTINCT bug, is_regression ' \
    'FROM metrics_changes WHERE file_id = ? AND release_id = ? AND is_regression = 1'
GET_REGRESSIONS_BY_RELEASE = 'SELECT DISTINCT bug, is_regression FROM metrics_changes WHERE release_id = ? AND is_regression = 1'
GET_REGRESSIONS = 'SELECT DISTINCT bug, file_id, release_id FROM metrics_changes WHERE is_regression = 1 ORDER BY file_id, release_id'
#GET_REGRESSIONS_BY_FILE_RELEASE  = 'SELECT DISTINCT bug, sum(is_regression) FROM metrics_changes WHERE file_id = ? AND release_id = ?'
GET_ALL_PEOPLE = 'SELECT name, bzemail, email, manager_email, department FROM metrics_people'
GET_COMMITTER_EMAIL = 'SELECT bzemail, email, manager_email, department FROM metrics_people WHERE email = ?'
GET_COMMITTER_BZEMAIL = 'SELECT bzemail, email, manager_email, department FROM metrics_people WHERE bzemail = ?'
GET_REGRESSION_BUG = 'SELECT bug, version, found, fixed, product, status, component, is_regression FROM metrics_bugs WHERE bug = ?'
GET_REGRESSION_RELEASE = 'SELECT bug, found, fixed, product, status, component, is_regression FROM metrics_bugs WHERE version = ?'
GET_ALL_BUGS = 'SELECT bug, version, found, fixed, product, status, component, is_regression, release_id FROM metrics_bugs'
GET_ALL_BUG_IDS = 'SELECT bug FROM metrics_bugs'
GET_MAX_VERSIONS = 'SELECT nightly, aurora, beta, release FROM metrics_release_schedule ' \
                   'WHERE start_date = (SELECT MAX(start_date) FROM metrics_release_schedule ' \
                   'WHERE start_date <= ?)'
GET_MAX_NIGHTLY = 'SELECT MAX(nightly) FROM metrics_release_schedule'
GET_PEOPLE_ID = 'SELECT people_id FROM metrics_people WHERE bzemail = ?'
GET_RELEASE_SCHEDULE = 'SELECT start_date, nightly, aurora, beta, release FROM metrics_release_schedule WHERE release > 24 ORDER by nightly'

GET_REGRESSION_COUNT ='SELECT regression_count, release_id  FROM metrics_bug_stats_view'
GET_BUG_COUNT = 'SELECT bug_count, release_id FROM metrics_bug_stats_view'
GET_REGRESSION_FIXED = 'SELECT regressions_fixed, release_id FROM metrics_bug_stats_view'
GET_BUG_FIXED = 'SELECT bugs_fixed, release_id FROM metrics_bug_stats_view'
GET_BACKOUT_COUNT = 'SELECT backout_count, release_id FROM metrics_bug_stats_view'



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
                #print 'TOTAL CHANGES:', self._dbconn.total_changes
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

        for v in create_view_stmts1:
            if not self._view_exists(v):
                self._run_execute(c, create_view_stmts1[v])
        for v in create_view_stmts2:
            if not self._view_exists(v):
                self._run_execute(c, create_view_stmts2[v])

    def _drop_tables(self):
        c = self._dbconn.cursor()
        for t in create_table_stmts:
            if self._table_exists(t):
                self._run_execute(c, DROP_TABLE_STMT % t)
        self._dbconn.commit()

    def _drop_views(self):
        c = self._dbconn.cursor()
        for t in create_view_stmts2:
            if self._view_exists(t):
                self._run_execute(c, DROP_VIEW_STMT % t)
        for t in create_view_stmts1:
            if self._view_exists(t):
                self._run_execute(c, DROP_VIEW_STMT % t)
        self._dbconn.commit()

    def _table_exists(self, name):
        c = self._dbconn.cursor()
        c = self._run_execute(c, TABLE_EXIST_STMT, [name])
        r = c.fetchone()
        return (r and (r[0] == name))

    def _view_exists(self, name):
        c = self._dbconn.cursor()
        c = self._run_execute(c, VIEW_EXIST_STMT, [name])
        r = c.fetchone()
        return (r and (r[0] == name))

    ###
    # Data Functions
    # Add data to tables
    # Get Data from tables
    def add_change_values(self, file_id, release_id, delta, total_lines, percent_change, bug, commit_id, is_backout, committer_name, reviewer, approver, msg, is_regression,  found, fixed):
        # Add entry to the METRICS_CHANGES table
        c = self._dbconn.cursor()
        self._run_execute(c, INSERT_CHANGES , [file_id, release_id, delta, total_lines, percent_change, bug, commit_id, is_backout, committer_name, reviewer, approver, msg, is_regression,  found, fixed])
        self._dbconn.commit()

    def add_file_values(self, file_name):
        # Add entry to the METRICS_FILES table
        c = self._dbconn.cursor()
        self._run_execute(c, INSERT_FILE_NAME, [file_name])
        self._dbconn.commit()
        return self.get_file_id(file_name)

    def add_release_values(self, release_name, release_number):
        # Add entry to the METRICS_FILES table
        c = self._dbconn.cursor()
        self._run_execute(c, INSERT_RELEASE, [release_name, release_number])
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
        c = self._run_execute(c,GET_BUGS_BY_FILE, [file_id])
        return c.fetchall()

    def get_bugs_by_file_release(self, file_id, release_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_BUGS_BY_FILE_RELEASE, [file_id, release_id])
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

    def add_person(self, name, bzemail, email, manager_email, deptname):
        c = self._dbconn.cursor()
        c = self._run_execute(c, INSERT_PEOPLE, (name, bzemail, email, manager_email, deptname))
        self._dbconn.commit()
        return self.get_people_id(bzemail)

    def update_person(self, bzemail, email, manager_email, deptname):
        c = self._dbconn.cursor()
        c = self._run_execute(c, INSERT_PEOPLE, (bzemail, email, manager_email, deptname))
        self._dbconn.commit()

    def get_all_people(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_ALL_PEOPLE)
        return c.fetchall()

    def add_bug(self, bug, version, found, fixed, product, status, component, is_regression, release_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c, INSERT_REGRESSION, (bug, version, found, fixed, product, status, component, is_regression, release_id))
        self._dbconn.commit()

    def get_regression_bug(self, bug):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_REGRESSION_BUG, [bug])
        return c.fetchall()

    def get_regression_release(self, version):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_REGRESSION_RELEASE, [version])
        return c.fetchall()

    def update_bug(self, bug, fixed, status, is_regression, release_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c, UPDATE_REGRESSIONS, (fixed, status, is_regression, release_id, bug))
        self._dbconn.commit()

    def update_bug_release_id(self, bug, release_id):
        c = self._dbconn.cursor()
        c = self._run_execute(c, UPDATE_BUG_RELEASE_ID, (release_id, bug))
        self._dbconn.commit() 

    def get_all_bug_ids(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_ALL_BUG_IDS)
        return c.fetchall()

    def get_all_bugs(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_ALL_BUGS)
        return c.fetchall()

    def get_max_versions(self, found):
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_MAX_VERSIONS, [found])
        return c.fetchall()

    def get_max_nightly():
        c = self._dbconn.cursor()
        c = self._run_execute(c,GET_MAX_NIGHTLY)
        return c.fetchone()

    def get_people_id(self, bzemail):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_PEOPLE_ID, [bzemail])
        return c.fetchone()

    def update_manager_id(self, manager_id, manager_email):
        c = self._dbconn.cursor()
        c = self._run_execute(c, UPDATE_MANAGER_ID ,[manager_id, manager_email])
        self._dbconn.commit()

    def get_release_schedule(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_RELEASE_SCHEDULE)
        return c.fetchall()

    def get_regession_count(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_REGRESSION_COUNT)
        return c.fetchall()

    def get_bug_count(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_BUG_COUNT)
        return c.fetchall()

    def get_regession_fixed(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_REGRESSION_FIXED)
        return c.fetchall()

    def get_bug_fixed(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_BUG_FIXED)
        return c.fetchall()

    def get_backout_count(self):
        c = self._dbconn.cursor()
        c = self._run_execute(c, GET_BACKOUT_COUNT)
        return c.fetchall()

